import os
import json
import pickle
import polars as pl
import pm4py
import logging
from typing import Dict, Any, List
from pm4py.objects.log.exporter.xes import exporter as xes_exporter

from ..application.errors import DataPreparationError, MissingDataError
from ..domain.interfaces import IResultWriter
from ..config import AnalysisConfig
from ..infrastructure.logging_config import LayerLoggerAdapter
from ..domain.types import KpiSummaryRecord


class FileResultWriter(IResultWriter):
    """
    Adapter del layer Infrastructure che implementa la porta di uscita IResultWriter.
    Fornisce la gestione delle operazioni di scrittura dei risultati su file system
    in modo tracciabile e con gestione uniforme delle eccezioni.
    """

    def __init__(self, config: AnalysisConfig):
        """
        Inizializza il writer configurando la directory base di output e il logger.
        """
        self.config = config
        os.makedirs(self.config.output_directory, exist_ok=True)

        base_logger = logging.getLogger(self.__class__.__name__)
        self.logger = LayerLoggerAdapter(base_logger, {"layer": "Infrastructure"})
        self.logger.info(f"FileResultWriter inizializzato. Directory di output: {self.config.output_directory}")

    # ============================================================
    # FASE 1 — Scrittura dei dataset di metriche e configurazioni
    # ============================================================

    def write_lazy_metrics_parquet(self, lf: pl.LazyFrame, filename: str) -> None:
        """
        Scrive sul disco il LazyFrame contenente le metriche grezze calcolate.
        Il file viene salvato in formato Parquet compresso.
        """
        output_path = os.path.join(self.config.output_directory, filename)
        self.logger.info(f"Avvio scrittura del LazyFrame di metriche in: {output_path}")

        try:
            lf.sink_parquet(output_path, compression="zstd")
            self.logger.info("Scrittura completata con successo per il file di metriche Parquet.")
        except Exception as e:
            self.logger.error(f"Errore durante la scrittura del file Parquet: {e}", exc_info=True)
            raise DataPreparationError(f"Impossibile salvare il file Parquet: {output_path}") from e

    def write_stratification_thresholds_json(self, thresholds: Dict[str, Any], filename: str) -> None:
        """
        Scrive sul disco il file JSON contenente le soglie di stratificazione
        calcolate dal dominio.
        """
        output_path = os.path.join(self.config.output_directory, filename)
        self.logger.info(f"Avvio scrittura del file JSON delle soglie di stratificazione in: {output_path}")

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(thresholds, f, indent=4)
            self.logger.info("File JSON delle soglie salvato correttamente.")
        except Exception as e:
            self.logger.error(f"Errore durante la scrittura del file JSON: {e}", exc_info=True)
            raise DataPreparationError(f"Errore di scrittura del file {filename}") from e

    def write_dataframe(self, df: pl.DataFrame, filename: str) -> None:
        """
        Scrive su disco un DataFrame Polars, in formato CSV o Parquet
        a seconda dell’estensione del file indicata.
        """
        output_path = os.path.join(self.config.output_directory, filename)
        self.logger.info(f"Avvio scrittura DataFrame in: {output_path}")

        try:
            if filename.endswith(".parquet"):
                df.write_parquet(output_path)
            else:
                df.write_csv(output_path)
            self.logger.info("Scrittura del DataFrame completata correttamente.")
        except Exception as e:
            self.logger.error(f"Errore durante la scrittura del DataFrame: {e}", exc_info=True)
            raise DataPreparationError(f"Impossibile scrivere il DataFrame su {output_path}") from e

    # ============================================================
    # FASE 2 — Scrittura degli artefatti di analisi di processo
    # ============================================================

    def write_process_analysis_artifacts(
        self,
        results: Dict[str, Any],
        repo_id: str,
        archetype_name: str,
        suffix: str = ""
    ) -> None:
        """
        Scrive su disco i modelli e i log generati durante l’analisi di processo
        per una singola repository. Ogni repository viene salvata in una
        directory dedicata organizzata per archetipo.
        """
        if not results:
            self.logger.warning(f"Nessun artefatto da salvare per la repository {repo_id}.")
            raise MissingDataError(f"Nessun risultato disponibile per {repo_id}.")

        safe_name = "".join(c for c in archetype_name if c.isalnum() or c in ("_", "-")).rstrip()
        output_dir = os.path.join(self.config.output_directory, "process_analysis", safe_name, str(repo_id))
        os.makedirs(output_dir, exist_ok=True)
        self.logger.info(f"Avvio scrittura artefatti di processo per '{repo_id}' (archetipo: {safe_name})...")

        try:
            freq_model = results.get("frequency_model")
            perf_model = results.get("performance_model")
            log_pm4py = results.get("filtered_log")
            suffix_str = f"_{suffix}" if suffix else ""

            if freq_model:
                path_pkl = os.path.join(output_dir, f"frequency_model{suffix_str}.pkl")
                with open(path_pkl, "wb") as f:
                    pickle.dump(freq_model, f)
                self.logger.info(f"Modello di frequenza salvato in: {path_pkl}")

                path_png = os.path.join(output_dir, f"frequency_model{suffix_str}.png")
                pm4py.save_vis_heuristics_net(freq_model, path_png)
                self.logger.info(f"Visualizzazione modello di frequenza salvata in: {path_png}")        

            if perf_model:
                path_pkl = os.path.join(output_dir, f"performance_model{suffix_str}.pkl")
                with open(path_pkl, "wb") as f:
                    pickle.dump(perf_model, f)
                self.logger.info(f"Modello di performance salvato in: {path_pkl}")

                path_png = os.path.join(output_dir, f"performance_model{suffix_str}.png")
                pm4py.save_vis_heuristics_net(perf_model, path_png)
                self.logger.info(f"Visualizzazione modello di performance salvata in: {path_png}") 

            if log_pm4py:
                path_xes = os.path.join(output_dir, f"filtered_log{suffix_str}.xes")
                xes_exporter.apply(log_pm4py, path_xes)
                self.logger.info(f"Log XES filtrato salvato correttamente in: {path_xes}")

        except Exception as e:
            self.logger.error(f"Errore durante la scrittura degli artefatti per {repo_id}: {e}", exc_info=True)
            raise DataPreparationError(f"Errore nel salvataggio degli artefatti per {repo_id}") from e

    # ============================================================
    # FASE 3 — Scrittura della sintesi quantitativa dei KPI
    # ============================================================

    def write_quantitative_summary(self, summary_data: List[KpiSummaryRecord]) -> None:
        """
        Prende i dati di sommario (come lista di dataclass), li converte
        in un DataFrame Polars e li salva nel percorso CSV definito dalla configurazione.
        """
        if not summary_data:
            return

        # 1. Converte la lista di dataclass in un DataFrame Polars (responsabilità dell'adapter)
        list_of_dicts = [record.__dict__ for record in summary_data]
        summary_df = pl.from_dicts(list_of_dicts)

        # 2. Ottiene il percorso dal config (responsabilità dell'adapter)
        output_path = self.config.quantitative_summary_file

        try:
            summary_df.write_csv(output_path)
            self.logger.info(f"Tabella di sintesi quantitativa salvata in: {output_path}")
        except Exception as e:
            self.logger.error(f"Errore durante il salvataggio della sintesi quantitativa: {e}")