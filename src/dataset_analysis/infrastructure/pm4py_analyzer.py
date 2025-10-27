import logging
import polars as pl
import pm4py
from typing import Optional, Any, Dict
from pm4py.objects.log.obj import EventLog

from ..domain.interfaces import IProcessAnalyzer
from ..config import AnalysisConfig
from ..application.errors import DataPreparationError, MissingDataError
from ..infrastructure.logging_config import LayerLoggerAdapter
from ..domain import predicates


class PM4PyAnalyzer(IProcessAnalyzer):
    """
    Adapter del layer Infrastructure che implementa la porta di uscita IProcessAnalyzer.
    Integra la libreria PM4Py per eseguire la discovery dei modelli di processo
    e restituire gli artefatti analitici derivanti dal log comportamentale
    di ciascuna repository.
    """

    def __init__(self):
        """
        Inizializza l'adapter configurando i parametri globali e il sistema di logging.
        """
        base_logger = logging.getLogger(self.__class__.__name__)
        self.logger = LayerLoggerAdapter(base_logger, {"layer": "Infrastructure"})
        self.logger.info("PM4PyAnalyzer inizializzato correttamente.")

    # ============================================================
    # FASE 1 — Analisi di processo per singola repository
    # ============================================================

    def analyze_repository(
        self,
        event_log_lazy: pl.LazyFrame,
        repository_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Esegue l'analisi di processo su una singola repository.
        La procedura comprende:
            1. Filtraggio degli eventi core del workflow
            2. Normalizzazione e formattazione del dataset
            3. Conversione in formato PM4Py (EventLog)
            4. Discovery dei modelli di processo (frequenza e performance)
        Restituisce un dizionario contenente i modelli e il log filtrato.
        """
        self.logger.info(f"[Analyzer] Avvio analisi PM4Py per repository {repository_id}...")

        # === Step 1: Filtro core (lazy) + materializzazione ===
        try:
            core_log_lazy = event_log_lazy.filter(predicates.is_core_workflow_event)
            repository_df = core_log_lazy.collect(engine="streaming")

            if repository_df.is_empty() or repository_df.height < 2:
                self.logger.warning(f"Log insufficiente o vuoto per {repository_id} dopo il filtro core.")
                return None

        # === Step 2: Pulizia e normalizzazione eventi ===
            repository_df = _normalize_event_names(repository_df)
            repository_df = _map_columns(repository_df)
            repository_df = _format_and_select_columns(repository_df)
            repository_df = repository_df.sort(["case:concept:name", "time:timestamp"])

        # === Step 3: Conversione in formato PM4Py ===
            event_log_pm4py = self._convert_df_to_pm4py_log(repository_df)
            if not event_log_pm4py:
                self.logger.warning(f"La conversione in EventLog ha prodotto un risultato vuoto per {repository_id}.")
                return None

        # === Step 4: Discovery del processo (Heuristics Miner) ===
            heuristics_net_freq = pm4py.discover_heuristics_net(event_log_pm4py)
            heuristics_net_perf = pm4py.discover_heuristics_net(event_log_pm4py, decoration="performance")

            self.logger.info(f"Analisi completata con successo per {repository_id}.")
            return {
                "frequency_model": heuristics_net_freq,
                "performance_model": heuristics_net_perf,
                "filtered_log": event_log_pm4py,
            }
        
        except Exception as e:
            self.logger.error(
                f"Analisi di processo fallita per repository {repository_id} a causa di un'eccezione: {e}",
                exc_info=True 
            )
            return None 

    # ============================================================
    # FASE 2 — Conversione in log PM4Py
    # ============================================================

    def _convert_df_to_pm4py_log(self, df: pl.DataFrame) -> EventLog:
        """
        Converte un DataFrame Polars formattato in un EventLog PM4Py.
        L'ordinamento e la selezione delle colonne assicurano la compatibilità
        con le funzioni di discovery della libreria.
        """
        pandas_df = df.to_pandas()
        formatted_df = pm4py.format_dataframe(
            pandas_df,
            case_id="case:concept:name",
            activity_key="concept:name",
            timestamp_key="time:timestamp"
        )
        return pm4py.convert_to_event_log(formatted_df)


# ============================================================
# FUNZIONI LOCALI DI SUPPORTO (pure e indipendenti dallo stato)
# ============================================================

def _map_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Mappa le colonne del DataFrame ai nomi standard del process mining."""
    return df.rename({
        "actor_id": "case:concept:name",
        "timestamp": "time:timestamp",
    })

def _normalize_event_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalizza i nomi degli eventi combinando 'activity' e campi payload specifici.
    """
    # Controlla dinamicamente la presenza di colonne opzionali per robustezza
    has_pr_merged = "pr_merged" in df.columns
    has_review_state = "review_state" in df.columns
    has_create_ref_type = "create_ref_type" in df.columns
    has_delete_ref_type = "delete_ref_type" in df.columns

    return df.with_columns(
        pl.when(pl.col("activity") == "PullRequestEvent")
        .then(
            pl.when((pl.col("action") == "closed") & (pl.col("pr_merged") == True) if has_pr_merged else False)
            .then(pl.lit("PullRequestEvent_merged"))
            .when((pl.col("action") == "closed") & (pl.col("pr_merged") == False) if has_pr_merged else False)
            .then(pl.lit("PullRequestEvent_rejected"))
            .otherwise(pl.col("activity").cast(pl.Utf8) + "_" + pl.col("action").cast(pl.Utf8))
        )
        .when(pl.col("activity") == "IssuesEvent")
        .then(pl.col("activity").cast(pl.Utf8) + "_" + pl.col("action").cast(pl.Utf8))
        .when(pl.col("activity") == "PullRequestReviewEvent")
        .then(
            pl.col("activity").cast(pl.Utf8) + "_" +
            (pl.col("review_state").cast(pl.Utf8) if has_review_state else pl.lit("unknown"))
        )
        .when(pl.col("activity").is_in(["IssueCommentEvent", "PullRequestReviewCommentEvent"]))
        .then(pl.col("activity").cast(pl.Utf8) + "_" + pl.col("action").cast(pl.Utf8))
        .when(pl.col("activity") == "CreateEvent")
        .then(
            pl.col("activity").cast(pl.Utf8) + "_" +
            (pl.col("create_ref_type").cast(pl.Utf8) if has_create_ref_type else pl.lit("ref"))
        )
        .when(pl.col("activity") == "DeleteEvent")
        .then(
            pl.col("activity").cast(pl.Utf8) + "_" +
            (pl.col("delete_ref_type").cast(pl.Utf8) if has_delete_ref_type else pl.lit("ref"))
        )
        .when(pl.col("activity") == "ReleaseEvent")
        .then(pl.col("activity").cast(pl.Utf8) + "_" + pl.col("action").cast(pl.Utf8))
        .otherwise(pl.col("activity").cast(pl.Utf8))
        .alias("concept:name")
    )

def _format_and_select_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Seleziona e formatta le colonne finali richieste da PM4Py."""
    df = df.with_columns(
        pl.col("case:concept:name").cast(pl.Utf8),
        pl.col("concept:name").cast(pl.Utf8),
        pl.col("case:concept:name").cast(pl.Utf8).alias("org:resource"),
        pl.col("time:timestamp").dt.replace_time_zone("UTC")
    )
    return df.select(["case:concept:name", "concept:name", "time:timestamp", "org:resource"])
