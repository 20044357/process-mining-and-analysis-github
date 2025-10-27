# File: src/dataset_analysis/domain/interfaces.py

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import polars as pl
from pm4py.objects.log.obj import EventLog

from ..domain.types import KpiSummaryRecord
from ..domain.types import (
    EventStream,
    RepositoryCreationTable,
    StratifiedRepositoriesDataset,
    AnalysisArtifacts,
)

# =====================================================================
#  I N T E R F A C E  –  D A T A  A C C E S S  &  O U T P U T
# =====================================================================

class IDataProvider(ABC):
    """
    Porta (port) del dominio per l’accesso ai dati delle repository e
    per la gestione delle sorgenti analitiche.  
    Definisce le operazioni che permettono di estrarre, caricare e organizzare
    i dati di input e gli artefatti di output del processo di analisi.
    """

    # ============================================================
    # FASE 1 — Eventi principali (Core Events)
    # ============================================================

    @abstractmethod
    def load_core_events(self) -> EventStream:
        """Carica il dataset principale di eventi in modalità lazy."""
        pass

    @abstractmethod
    def load_raw_repo_creation_events(self) -> RepositoryCreationTable:
        """Carica gli eventi grezzi di creazione delle repository."""
        pass

    @abstractmethod
    def load_repository_events(self, repo_id: str) -> EventStream:
        """Carica in modalità lazy tutti gli eventi di una specifica repository."""
        pass

    # ============================================================
    # FASE 2 — Risultati stratificati
    # ============================================================

    @abstractmethod
    def load_stratified_repositories(self) -> StratifiedRepositoriesDataset:
        """Carica i risultati della stratificazione delle repository."""
        pass

    # ============================================================
    # FASE 3 — Artefatti di analisi
    # ============================================================

    @abstractmethod
    def scan_analysis_artifacts(self) -> List[AnalysisArtifacts]:
        """Scansiona la directory di output per individuare gli artefatti di analisi."""
        pass


class IResultWriter(ABC):
    """
    Porta (port) del dominio per il salvataggio dei risultati generati
    durante le fasi di analisi e sintesi della pipeline.
    Definisce le operazioni di scrittura degli artefatti e dei risultati
    finali in formato persistente.
    """

    # ============================================================
    # FASE 1 — Scrittura dataset e configurazioni
    # ============================================================

    @abstractmethod
    def write_lazy_metrics_parquet(self, lf: pl.LazyFrame, filename: str):
        """Scrive su disco un LazyFrame di metriche in formato Parquet."""
        pass

    @abstractmethod
    def write_dataframe(self, df: pl.DataFrame, filename: str):
        """Scrive un DataFrame in formato CSV o Parquet a seconda dell’estensione."""
        pass

    @abstractmethod
    def write_stratification_thresholds_json(self, thresholds: Dict[str, Any], filename: str):
        """Scrive un file JSON contenente le soglie di stratificazione."""
        pass

    # ============================================================
    # FASE 2 — Artefatti di processo
    # ============================================================

    @abstractmethod
    def write_process_analysis_artifacts(
        self,
        results: Dict[str, Any],
        repo_id: str,
        archetype_name: str,
        suffix: str = ""
    ):
        """Scrive i modelli e i log prodotti durante l'analisi di processo."""
        pass

    # ============================================================
    # FASE 3 — Risultati sintetici
    # ============================================================

    @abstractmethod
    def write_quantitative_summary(self, summary_data: List[KpiSummaryRecord]):
        """Scrive la tabella finale dei KPI aggregati in formato CSV."""
        pass


# =====================================================================
#  I N T E R F A C E  –  P R O C E S S  &  M O D E L  A N A L Y S I S
# =====================================================================

class IProcessAnalyzer(ABC):
    """
    Porta (port) del dominio per la discovery e l’analisi dei processi.
    Definisce l’interfaccia per gli adapter che integrano librerie di
    process mining (es. PM4Py) e restituiscono i modelli analitici.
    """

    @abstractmethod
    def analyze_repository(
        self, event_log_lazy: pl.LazyFrame, repository_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Esegue l’analisi di processo su una singola repository.
        Restituisce un dizionario con i risultati (modelli, log, metriche)
        o None se l’analisi fallisce.
        """
        pass


class IModelAnalyzer(ABC):
    """
    Porta (port) del dominio per l’ispezione e l’analisi dei modelli di processo.
    Definisce i metodi per caricare modelli di tipo HeuristicsNet e per
    calcolare KPI strutturali, frequenziali e prestazionali.
    """

    # ============================================================
    # FASE 1 — Caricamento modello
    # ============================================================

    @abstractmethod
    def load_model_from_file(self, file_path: str) -> Optional[Any]:
        """
        Carica un modello di processo da un file (es. pickle).
        Restituisce un oggetto modello o None se il caricamento fallisce.
        """
        pass

    # ============================================================
    # FASE 2 — KPI Strutturali
    # ============================================================

    @abstractmethod
    def count_activities(self, model: Any) -> Optional[int]:
        """(KPI di Supporto) Conta il numero di attività (nodi) nel modello."""
        pass

    @abstractmethod
    def count_connections(self, model: Any) -> Optional[int]:
        """(KPI di Supporto) Conta il numero di connessioni (archi) nel modello."""
        pass

    @abstractmethod
    def is_issue_driven(self, model: Any) -> Optional[bool]:
        """[KPI #1] Verifica se il processo è guidato dalle issue della community."""
        pass

    @abstractmethod
    def has_review_loop(self, model: Any) -> Optional[bool]:
        """[KPI #4] Verifica la presenza di un ciclo di review (Feedback → Correzione → Feedback)."""
        pass

    # ============================================================
    # FASE 3 — KPI di Frequenza
    # ============================================================

    @abstractmethod
    def get_rework_intensity(self, freq_model: Any) -> Optional[float]:
        """[KPI #5] Misura l’intensità del dibattito (forza del ciclo di rework)."""
        pass

    @abstractmethod
    def get_pr_success_rate(self, freq_model: Any) -> Optional[float]:
        """(KPI di Supporto) Percentuale di Pull Request unite con successo."""
        pass

    # ============================================================
    # FASE 4 — KPI di Performance
    # ============================================================

    @abstractmethod
    def get_issue_reaction_time_hours(self, perf_model: Any) -> Optional[float]:
        """[KPI #2] Latenza diretta (ore) tra apertura di un’Issue e apertura di una PR."""
        pass

    @abstractmethod
    def get_direct_merge_latency_hours(self, perf_model: Any) -> Optional[float]:
        """[KPI #6, Variante] Latenza diretta (ore) tra apertura di una PR e merge."""
        pass


class ILogAnalyzer(ABC):
    """
    Porta (port) del dominio per l’analisi dei log comportamentali.
    Definisce le operazioni di caricamento e ispezione di un EventLog
    per il calcolo dei KPI di partecipazione e temporali.
    """

    # ============================================================
    # FASE 1 — Caricamento log
    # ============================================================

    @abstractmethod
    def load_log_from_file(self, file_path: str) -> Optional[EventLog]:
        """
        Carica un log di eventi da file (es. .xes o formato equivalente).
        Restituisce un oggetto EventLog o None se il caricamento fallisce.
        """
        pass

    # ============================================================
    # FASE 2 — KPI di Partecipazione
    # ============================================================

    @abstractmethod
    def get_num_active_actors(self, log: EventLog) -> Optional[int]:
        """[KPI #7] Numero di attori attivi (tracce) presenti nel log."""
        pass

    @abstractmethod
    def get_activity_concentration(self, log: EventLog) -> Optional[float]:
        """[KPI #8] Indice di concentrazione dell’attività (0–1)."""
        pass

    # ============================================================
    # FASE 3 — KPI Temporali
    # ============================================================

    @abstractmethod
    def get_true_pr_lead_time_hours(self, log: EventLog) -> Optional[float]:
        """[KPI #6, Vero] Lead time totale mediano di una PR (in ore)."""
        pass

    @abstractmethod
    def get_fork_to_pr_conversion_rate(self, log: EventLog) -> Optional[float]:
        """[KPI #3] Tasso di conversione da Fork a Pull Request."""
        pass
