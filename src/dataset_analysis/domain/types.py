"""
Modulo: domain.types

Definisce i tipi semantici condivisi tra i layer dell’architettura
(dominio ↔ application ↔ infrastructure).

Questi tipi rappresentano astrazioni logiche (es. "EventStream", "MetricsPlan"),
non implementazioni concrete.  
Gli adapter di infrastruttura possono restituire oggetti equivalenti
(es. Polars, Pandas, ecc.) senza modificare il contratto del dominio.
"""

from typing import Any, Optional, Dict, List
from dataclasses import dataclass


# =====================================================================
# Tipi logici di alto livello (alias semantici)
# =====================================================================

EventStream = Any
"""
Flusso di eventi processato in modalità lazy.
Può corrispondere a un `polars.LazyFrame`, un generatore o un oggetto
equivalente in altri backend analitici.
"""

RepositoryCreationTable = Any
"""
Tabella di eventi di creazione repository (tipicamente `polars.DataFrame`).
Contiene almeno le colonne: `repo_id`, `timestamp` o `repo_creation_date`.
"""

MetricsPlan = Any
"""
Piano di calcolo lazy per le metriche di riepilogo.
Corrisponde a un `LazyFrame` Polars non materializzato.
"""

StratificationPlan = Any
"""
Piano di calcolo lazy che applica le soglie di stratificazione
per generare le categorie normalizzate e lo `strato_id`.
"""

StratifiedRepositoriesDataset = Any
"""
Dataset finale contenente le repository stratificate secondo i quantili.
Generalmente rappresentato come `polars.DataFrame`.
"""


# =====================================================================
# Data Transfer Objects (DTO) — Strutture dati di dominio
# =====================================================================

@dataclass
class AnalysisArtifacts:
    """
    Rappresenta l’insieme di artefatti analitici generati per una singola repository.
    Ogni artefatto collega i modelli e i log associati all’archetipo di riferimento.

    Attributi
    ----------
    repo_id : str
        Identificativo univoco della repository.
    archetype_name : str
        Nome dell’archetipo a cui appartiene la repository.
    freq_model_path : str
        Percorso del modello di frequenza salvato in formato pickle.
    perf_model_path : str
        Percorso del modello di performance salvato in formato pickle.
    log_path : str
        Percorso del log comportamentale in formato XES.
    """
    repo_id: str
    archetype_name: str
    freq_model_path: str
    perf_model_path: str
    log_path: str


@dataclass
class KpiSummaryRecord:
    """
    Rappresenta una singola riga della tabella di sintesi quantitativa finale.
    Ogni record contiene i KPI strutturali, di performance e di attività
    calcolati per una repository e il relativo archetipo.

    Attributi
    ----------
    repo_id : str
        Identificativo della repository.
    archetype : str
        Nome dell’archetipo di appartenenza.
    num_attori_attivi : Optional[int]
        Numero di attori attivi (KPI #7).
    activity_concentration : Optional[float]
        Indice di concentrazione dell’attività (KPI #8).
    pr_lead_time_total_hours : Optional[float]
        Tempo mediano totale (in ore) tra apertura e merge PR (KPI #6, vero).
    is_issue_driven : Optional[bool]
        True se il processo è guidato da issue aperte (KPI #1).
    has_review_loop : Optional[bool]
        True se è presente un ciclo di revisione tecnico (KPI #4).
    rework_intensity : Optional[float]
        Forza del ciclo di revisione (KPI #5).
    pr_success_rate : Optional[float]
        Percentuale di Pull Request unite con successo.
    num_activities : Optional[int]
        Numero di attività (nodi) nel modello.
    num_connections : Optional[int]
        Numero di connessioni (archi) nel grafo.
    issue_reaction_time_hours : Optional[float]
        Tempo medio (in ore) tra apertura issue e apertura PR (KPI #2).
    direct_merge_latency_hours : Optional[float]
        Latenza diretta di merge PR → merge (KPI #6, variante).
    """
    repo_id: str
    archetype: str
    num_attori_attivi: Optional[int]
    activity_concentration: Optional[float]
    pr_lead_time_total_hours: Optional[float]
    is_issue_driven: Optional[bool]
    has_review_loop: Optional[bool]
    rework_intensity: Optional[float]
    pr_success_rate: Optional[float]
    num_activities: Optional[int]
    num_connections: Optional[int]
    issue_reaction_time_hours: Optional[float]
    direct_merge_latency_hours: Optional[float]


# =====================================================================
# Strutture di output per il campionamento (fase finale del dominio)
# =====================================================================

@dataclass
class SamplingInfo:
    """
    Contiene i risultati dettagliati del campionamento
    per un singolo archetipo.

    Attributi
    ----------
    stats_table : Any
        Tabella con le statistiche descrittive per ciascuna combinazione di categorie.
        Tipicamente un `polars.DataFrame`.
    selected_repos : List[Dict[str, Any]]
        Lista di dizionari con le repository selezionate e le relative metriche.
    """
    stats_table: Any
    selected_repos: List[Dict[str, Any]]


@dataclass
class SamplingReport:
    """
    Report completo della selezione di repository rappresentative
    per tutti gli archetipi.

    Attributi
    ----------
    sampled_repo_ids : Dict[str, List[str]]
        Mappa che associa ogni archetipo alla lista di repository selezionate.
    detailed_reports : Dict[str, SamplingInfo]
        Mappa con i dettagli analitici per ciascun archetipo
        (statistiche e repository rappresentative).
    """
    sampled_repo_ids: Dict[str, List[str]]
    detailed_reports: Dict[str, SamplingInfo]
