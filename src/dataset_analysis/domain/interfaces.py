from abc import ABC, abstractmethod
from typing import List, Set, Dict, Any, Optional, Tuple
import polars as pl
from pm4py.objects.log.obj import EventLog
from ..domain.entities import RepositoryMetrics

class IDataProvider(ABC):
    """Porta per l'accesso ai dati delle repository."""
    @abstractmethod
    def get_eligible_repo_ids(self) -> Set[str]:
        pass

    @abstractmethod
    def calculate_summary_metrics_with_age(self, repo_ids: Set[str]) -> pl.LazyFrame:
        pass

    @abstractmethod
    def calculate_summary_metrics_gold(self, repo_ids: Set[str]) -> pl.LazyFrame:
        pass

    @abstractmethod
    def calculate_summary_metrics(self, repo_ids: Set[str]) -> pl.LazyFrame:
        pass

    @abstractmethod
    def calculate_summary_metrics_from_file(self, repo_ids_path: str) -> List[RepositoryMetrics]: # <-- CAMBIO: Restituisce una lista di entità
        pass

    @abstractmethod
    def calculate_summary_metrics_golden_recipe(sel, repo_ids: Set[str]) -> pl.LazyFrame:
        pass

    @abstractmethod
    def get_lazy_events_for_repo(self, repo_id: str) -> pl.LazyFrame:
        pass

    @abstractmethod
    def get_stratified_data(self) -> pl.DataFrame:
        """
        Recupera il DataFrame completo dei dati stratificati.
        L'implementazione deciderà DA DOVE recuperarli (file, db, cache...).
        """
        pass

    @abstractmethod
    def get_events_for_repo_split_by_peak(self, repo_id: str, peak_event_type: str) -> Optional[Tuple[pl.LazyFrame, pl.LazyFrame]]:
        pass

class IProcessAnalyzer(ABC):
    """Porta per l'esecuzione dell'analisi di process mining."""
    @abstractmethod
    def analyze_repo(self, repo_lazy_frame: pl.LazyFrame, repo_id: str, strato_id: int) -> Optional[Tuple[Dict[str, Any], Any, Any, EventLog]]:
        pass

    @abstractmethod
    def analyze_repo_new(self, repo_lazy_frame: pl.LazyFrame, repo_id: str) -> Optional[Tuple[Any, Any, EventLog]]:
        pass

    @abstractmethod
    def analyze_repo_process(self, event_log_lazy: pl.LazyFrame, repo_id: str) -> Optional[Dict[str, Any]]:
        pass

class IResultWriter(ABC):
    """Porta per il salvataggio dei risultati dell'analisi."""
    @abstractmethod
    def save_entities_as_dataframe(self, entities: List, filename: str):
        pass
    
    @abstractmethod
    def save_lazyframe(self, lf: pl.LazyFrame, filename: str):
        pass

    @abstractmethod
    def save_polars_csv(self, df: pl.DataFrame, filename: str):
        pass

    @abstractmethod
    def save_dataframe(self, df: pl.DataFrame, filename: str):
        pass

    @abstractmethod
    def save_json(self, data: Dict, filename: str):
        pass

    @abstractmethod
    def save_id_list(self, ids: List[str], filename: str):
        pass

    @abstractmethod
    def save_analysis_artifacts(self, results: Dict[str, Any], repo_id: str, archetype_name: str, suffix: str = ""):
        pass

    @abstractmethod
    def save_log_xes(self, log: EventLog, repo_id: str, strato_id: int):
        pass

    @abstractmethod
    def save_heuristics_net(self, heu_net: Any, repo_id: str, strato_id: int, suffix: str):
        pass

    @abstractmethod
    def save_kpis(self, kpis: List[Dict[str, Any]]):
        pass

    @abstractmethod
    def save_quantitative_summary(self, summary_df: pl.DataFrame, filename: str):
        """Salva la tabella di sintesi quantitativa finale in formato CSV."""
        pass

class IModelAnalyzer(ABC):
    """
    Interfaccia per un servizio che estrae metriche quantitative
    da modelli di processo già scoperti.
    """
    @abstractmethod
    def load_model_from_file(self, file_path: str) -> Optional[Any]:
        """Carica un modello di processo da un file (es. pickle o xes)."""
        pass

    @abstractmethod
    def count_activities(self, model: Any) -> int:
        """Conta il numero di attività (nodi) in un modello."""
        pass

    @abstractmethod
    def count_connections(self, model: Any) -> int:
        """Conta il numero di connessioni (archi) in un modello."""
        pass

    @abstractmethod
    def has_review_loop(self, model: Any) -> bool:
        """Verifica la presenza di un ciclo di review (es. Review -> Push -> Review)."""
        pass

    @abstractmethod
    def get_review_cycle_performance(self, model: Any) -> Optional[float]:
        """
        Estrae il tempo mediano (in ore) del ciclo di review, se esiste.
        Ritorna None se il ciclo non è presente.
        """
        pass

    # =====================================================================
    # --- AGGIORNAMENTO: Aggiungi qui le firme dei nuovi metodi ---
    # =====================================================================

    @abstractmethod
    def is_issue_driven(self, model: Any) -> bool:
        """
        Verifica se il processo è guidato dalle issue della community.
        """
        pass

    @abstractmethod
    def get_issue_reaction_time(self, model: Any) -> Optional[float]:
        """
        Estrae il tempo mediano (in ore) tra l'apertura di un'issue e l'apertura di una PR.
        """
        pass

    @abstractmethod
    def get_pr_lead_time(self, model: Any) -> Optional[float]:
        """
        Estrae il tempo mediano (in ore) tra l'apertura di una PR e il suo merge.
        """
        pass