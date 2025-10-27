# File: src/dataset_analysis/infrastructure/data_provider.py

from datetime import datetime, timedelta
import os
from pathlib import Path
import polars as pl
import logging
from typing import List

from ..domain.interfaces import IDataProvider
from ..domain.predicates import is_repo_creation_event
from ..application.errors import DataPreparationError, MissingDataError
from ..infrastructure.logging_config import LayerLoggerAdapter
from ..domain.types import AnalysisArtifacts
from ..domain import constants as domain_constants


class ParquetDataProvider(IDataProvider):
    """
    Adapter del layer Infrastructure che implementa la porta di uscita IDataProvider.
    Fornisce accesso ai dataset Parquet di eventi e ai risultati derivati,
    con gestione uniforme di logging ed eccezioni.
    """

    def __init__(
        self,
        dataset_directory: str,
        start_date: str,
        end_date: str,
        stratified_repositories_file: str,
        output_directory: str,
    ):
        """
        Inizializza il provider con i percorsi e i parametri temporali
        definiti nella configurazione dell’analisi.
        """
        self.dataset_directory = dataset_directory
        self.start_date = start_date
        self.end_date = end_date
        self.stratified_repositories_file = stratified_repositories_file
        self.output_directory = output_directory

        base_logger = logging.getLogger(self.__class__.__name__)
        self.logger = LayerLoggerAdapter(base_logger, {"layer": "Infrastructure"})
        self.logger.info(f"ParquetDataProvider inizializzato. Dataset: {self.dataset_directory}")

    # ============================================================
    # METODI PRIVATI — Accesso al dataset sorgente
    # ============================================================

    def _generate_daily_dataset_paths(self) -> List[str]:
        """
        Genera i pattern dei percorsi giornalieri per i file Parquet
        usando i parametri di configurazione della classe.
        """
        try:
            start_dt = datetime.fromisoformat(self.start_date.replace("Z", "")).date()
            end_dt = datetime.fromisoformat(self.end_date.replace("Z", "")).date()
        except ValueError:
            raise ValueError(f"Date non valide: start='{self.start_date}', end='{self.end_date}'")

        paths = []
        current_date = start_dt
        while current_date <= end_dt:
            pattern = (
                Path(self.dataset_directory)
                / f"anno={current_date.year}"
                / f"mese={current_date.month:02d}"
                / f"giorno={current_date.day:02d}"
                / "*.parquet"
            )
            paths.append(str(pattern))
            current_date += timedelta(days=1)
        return paths

    def _scan_source_dataset(self) -> pl.LazyFrame:
        """
        Costruisce un LazyFrame Polars efficiente scansionando solo i file
        nel range temporale richiesto, sfruttando la struttura partizionata.
        """
        try:
            date_paths = self._generate_daily_dataset_paths()
            if not date_paths:
                raise FileNotFoundError(
                    f"Nessun percorso file generato nel range {self.start_date} - {self.end_date}"
                )

            self.logger.info(
                f"Generati {len(date_paths)} pattern di percorsi per la scansione Parquet."
            )
            return pl.scan_parquet(date_paths)

        except (ValueError, FileNotFoundError) as e:
            self.logger.error(
                f"Errore durante la generazione dei percorsi o la scansione: {e}",
                exc_info=True,
            )
            raise DataPreparationError("Impossibile accedere al dataset sorgente.") from e

    # ============================================================
    # FASE 1 — Accesso e caricamento degli eventi principali
    # ============================================================

    def load_core_events(self) -> pl.LazyFrame:
        """
        Carica il dataset Parquet principale in modalità lazy,
        limitato all’intervallo temporale di analisi.
        Esegue la validazione tecnica dello schema e gestisce
        eventuali errori di formato o assenza dei file.
        """
        self.logger.info("Avvio scansione degli eventi principali...")
        try:
            lf = self._scan_source_dataset()

            # Validazione minima dello schema
            required_cols = {"repo_id", "activity", "timestamp"}
            available_cols = set(lf.collect_schema().names())
            missing = required_cols - available_cols
            if missing:
                self.logger.error(f"Colonne mancanti nel dataset: {missing}")
                raise DataPreparationError(f"Dataset non conforme: mancano {missing}")

            self.logger.info("Scansione completata con successo.")
            return lf

        except FileNotFoundError as e:
            self.logger.error("Nessun file Parquet trovato durante la scansione.", exc_info=True)
            raise MissingDataError("File sorgenti non trovati.") from e

        except ValueError as e:
            self.logger.error("Formato data non valido.", exc_info=True)
            raise DataPreparationError("Le date non rispettano il formato ISO richiesto.") from e

    def load_raw_repo_creation_events(self) -> pl.DataFrame:
        """
        Carica gli eventi grezzi di creazione delle repository
        all’interno dell’intervallo di analisi.
        """
        self.logger.info("Avvio caricamento eventi grezzi di creazione repository...")
        try:
            dset = self.load_core_events()
            raw_events_df = (
                dset.filter(is_repo_creation_event)
                .select("repo_id", "timestamp")
                .collect(engine="streaming")
            )

            if raw_events_df.is_empty():
                self.logger.warning("Nessun evento di creazione repository trovato.")
            else:
                self.logger.info(f"Caricati {len(raw_events_df):,} eventi di creazione repository.")

            return raw_events_df

        except Exception as e:
            self.logger.error("Errore durante il caricamento degli eventi di creazione.", exc_info=True)
            raise DataPreparationError("Errore durante l’accesso ai file Parquet sorgente.") from e

    def load_repository_events(self, repo_id: str) -> pl.LazyFrame:
        """
        Carica in modalità lazy tutti gli eventi associati a una specifica repository.
        Utilizzato per analisi di processo puntuali o di confronto.
        """
        self.logger.info(f"Avvio caricamento lazy degli eventi per repository {repo_id}...")

        try:
            date_paths = self._generate_daily_dataset_paths()
        except Exception as e:
            self.logger.error(f"Errore nella generazione dei percorsi per {repo_id}: {e}", exc_info=True)
            return pl.LazyFrame()

        if not date_paths:
            self.logger.warning("Nessun percorso file trovato. Restituzione LazyFrame vuoto.")
            return pl.LazyFrame()

        lazy_frame = (
            pl.scan_parquet(date_paths)
            .filter(pl.col("repo_id") == int(repo_id))
            .filter(~pl.col("actor_login").str.ends_with("[bot]").fill_null(False))
            .with_columns(pl.col("repo_id").cast(pl.Utf8))
        )

        self.logger.info(f"LazyFrame creato correttamente per repository {repo_id}.")
        return lazy_frame

    # ============================================================
    # FASE 2 — Caricamento dei risultati stratificati
    # ============================================================

    def load_stratified_repositories(self) -> pl.DataFrame:
        """
        Carica i risultati finali della stratificazione delle repository
        da un file Parquet generato in precedenza.
        """
        file_path = self.stratified_repositories_file
        self.logger.info(f"Caricamento risultati stratificati da: {file_path}")

        try:
            if not os.path.exists(file_path):
                raise MissingDataError(f"File dei risultati stratificati non trovato: {file_path}")

            stratified_df = pl.read_parquet(file_path)

            required_cols = {"repo_id", "strato_id", "age_in_days"}
            available_cols = set(stratified_df.columns)
            missing = required_cols - available_cols
            if missing:
                self.logger.error(f"File stratificato non conforme. Colonne mancanti: {missing}")
                raise DataPreparationError(
                    f"Schema Parquet non valido: colonne mancanti {missing} in {file_path}"
                )

            if stratified_df.is_empty():
                self.logger.warning("Il file stratificato è stato caricato ma risulta vuoto.")
            else:
                self.logger.info(f"Caricati {stratified_df.height:,} record stratificati con schema valido.")

            return stratified_df

        except (MissingDataError, DataPreparationError):
            raise
        except Exception as e:
            self.logger.error(f"Errore durante il caricamento del file stratificato: {e}", exc_info=True)
            raise DataPreparationError(f"Impossibile leggere il file stratificato {file_path}") from e

    # ============================================================
    # FASE 3 — Scansione degli artefatti di analisi
    # ============================================================

    def scan_analysis_artifacts(self) -> List[AnalysisArtifacts]:
        """
        Scansiona la directory di output alla ricerca degli artefatti
        generati durante le analisi di processo (modelli e log).
        Restituisce un elenco strutturato di AnalysisArtifacts.
        """
        results_dir = os.path.join(self.output_directory, "process_analysis")
        self.logger.info(f"Scansione artefatti di analisi in: {results_dir}")

        if not os.path.isdir(results_dir):
            self.logger.warning(f"La directory '{results_dir}' non esiste. Nessun artefatto trovato.")
            return []

        all_artifacts: List[AnalysisArtifacts] = []

        for archetype_name in os.listdir(results_dir):
            archetype_dir = os.path.join(results_dir, archetype_name)
            if not os.path.isdir(archetype_dir):
                continue

            for repo_id in os.listdir(archetype_dir):
                repo_dir = os.path.join(archetype_dir, repo_id)
                if not os.path.isdir(repo_dir):
                    continue

                freq_model_path = os.path.join(repo_dir, domain_constants.FREQ_MODEL_SUFFIX)
                perf_model_path = os.path.join(repo_dir, domain_constants.PERF_MODEL_SUFFIX)
                log_path = os.path.join(repo_dir, domain_constants.LOG_FILE_SUFFIX)

                if all(os.path.exists(p) for p in [freq_model_path, perf_model_path, log_path]):
                    all_artifacts.append(
                        AnalysisArtifacts(
                            repo_id=repo_id,
                            archetype_name=archetype_name,
                            freq_model_path=freq_model_path,
                            perf_model_path=perf_model_path,
                            log_path=log_path,
                        )
                    )

        self.logger.info(f"Scansione completata. Artefatti trovati: {len(all_artifacts)}")
        return all_artifacts
