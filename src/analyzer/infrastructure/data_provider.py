from datetime import datetime, timedelta
import os
from pathlib import Path
import pickle
import polars as pl
import logging
from typing import Any, Counter, List, Optional, Set, Union

from ..domain.interfaces import IDataProvider
from ..domain.predicates import is_repo_creation_event
from ..application.errors import DataPreparationError, MissingDataError

class ParquetDataProvider(IDataProvider):
    def __init__(
        self,
        dataset_directory: str,
        start_date: str,
        end_date: str,
        analyzable_repositories_file: str,
        stratified_repositories_file: str,
        output_directory: str,
        aggregate_model_subdirectory_name: str,
        archetype_process_models_directory: str,
        logger: Optional[Union[logging.Logger, logging.LoggerAdapter]] = None

    ):
        self.dataset_directory = dataset_directory
        self.start_date = start_date
        self.end_date = end_date
        self.analyzable_repositories_file = analyzable_repositories_file
        self.stratified_repositories_file = stratified_repositories_file
        self.output_directory = output_directory
        self.aggregate_model_subdirectory_name = aggregate_model_subdirectory_name
        self.archetype_process_models_directory = archetype_process_models_directory
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.logger.info(f"ParquetDataProvider inizializzato. Dataset: {self.dataset_directory}")


    def load_single_archetype_model(
        self, 
        archetype_name: str, 
        model_type: str = "frequency"
    ) -> Optional[Any]: 
        self.logger.info(f"Tentativo di caricamento del modello '{model_type}' per l'archetipo '{archetype_name}'...")

        try:
            input_dir = self._get_or_create_archetype_path(archetype_name)
            base_filename = archetype_name.lower().replace(" ", "_")
            input_path = input_dir / f"{base_filename}_{model_type}.pkl"
            self.logger.info(f"Caricamento del modello di processo {model_type} per '{archetype_name}' da: {input_path}")

            if not input_path.exists():
                self.logger.warning(f"Modello di processo {model_type} per '{archetype_name}' da: {input_path} non trovato")
                return None

            with open(input_path, "rb") as f:
                loaded_model = pickle.load(f)
  
            self.logger.info(f"Modello di processo {model_type} per '{archetype_name}' caricato con successo")
            return loaded_model

        except (pickle.UnpicklingError, EOFError) as e:
            self.logger.error(f"Errore di deserializzazione per il file: {e}", exc_info=True)
            return None
        except Exception as e:
            self.logger.error(f"Errore imprevisto durante il caricamento del modello per '{archetype_name}': {e}", exc_info=True)
            return None

    def build_aggregates_lazyframe(
        self,
        archetype_repo_list: list[int]
    ) -> pl.LazyFrame:
        cols = {
            "repo": "repo_id", "case": "actor_id",
            "activity": "activity", "timestamp": "timestamp"
        }
        columns_to_read = list(set(
            list(cols.values()) + [
                "action", "pr_merged", "review_state", "create_ref_type", "delete_ref_type", "actor_login"
            ]
        ))
        
        date_paths = sorted(self._generate_daily_dataset_paths())
        if not date_paths:
            self.logger.warning("Nessun file Parquet trovato.")
            return pl.LazyFrame()
        
        ldf = (
            pl.scan_parquet(date_paths)
            .select(columns_to_read)
            .filter(pl.col("repo_id").is_in(archetype_repo_list))
        )

        return ldf

    def load_core_events(self) -> pl.LazyFrame:
        try:
            lf = self._scan_source_dataset()

            required_cols = {"repo_id", "activity", "timestamp"}
            available_cols = set(lf.collect_schema().names())
            missing = required_cols - available_cols
            if missing:
                self.logger.error(f"Colonne mancanti nel dataset: {missing}")
                raise DataPreparationError(f"Dataset non conforme: mancano {missing}")

            self.logger.info("Scansione del dataset completata con successo.")
            return lf

        except FileNotFoundError as e:
            self.logger.error("Nessun file Parquet trovato durante la scansione.", exc_info=True)
            raise MissingDataError("File sorgenti non trovati.") from e

        except ValueError as e:
            self.logger.error("Formato data non valido.", exc_info=True)
            raise DataPreparationError("Le date non rispettano il formato richiesto.") from e

    def load_raw_repo_creation_events(self) -> pl.DataFrame:
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
            raise DataPreparationError("Errore durante l'accesso ai file Parquet sorgente.") from e
            
    def load_stratified_repositories(self) -> pl.DataFrame:
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

    def _get_or_create_archetype_path(self, archetype_name: str) -> Path:
        base_dir = Path(self.archetype_process_models_directory)
        
        archetype_path = base_dir / archetype_name
        archetype_path.mkdir(parents=True, exist_ok=True)
        
        return archetype_path
    
    def _generate_daily_dataset_paths(self) -> List[str]:
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
        try:
            date_paths = self._generate_daily_dataset_paths()
            if not date_paths:
                raise FileNotFoundError(
                    f"Nessun percorso file generato nel range {self.start_date} - {self.end_date}"
                )

            return pl.scan_parquet(date_paths)

        except (ValueError, FileNotFoundError) as e:
            self.logger.error(
                f"Errore durante la generazione dei percorsi o la scansione: {e}",
                exc_info=True,
            )
            raise DataPreparationError("Impossibile accedere al dataset sorgente.") from e
