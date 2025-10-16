# File: src/dataset_analysis/domain/entities.py

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List

# Questa classe non è strettamente usata nella pipeline attuale (che usa Polars DF)
# ma definisce il "linguaggio" del dominio.
@dataclass
class RepositoryMetrics:
    """Rappresenta le metriche 'Gold Standard' di una repository."""
    repo_id: str
    volume_lavoro: int
    intensita_collaborativa: int
    engagement_community: int
    strato_id: Optional[int] = None # Verrà popolato dopo, dai servizi di dominio

@dataclass
class ProcessKPIs:
    """
    Rappresenta il vettore di feature quantitative estratte dal process mining.
    Questa entità serve a dare un tipo ben definito al dizionario di KPI.
    """
    # Metadata
    repo_id: str
    strato_id: int
    
    # Categoria A: Scala e Contesto
    num_eventi: int
    num_attori: int
    num_nodi: int
    num_archi: int
    
    # Categoria B: Workflow e Formalità
    is_pr_based: int
    pr_success_rate: Optional[float]
    uses_release: int
    branch_cleanup_rate: Optional[float]
    is_issue_driven: int
    
    # Categoria C: Collaborazione e Qualità
    rework_intensity: float
    review_severity: Optional[float]
    
    # Categoria D: Performance e Tempi
    time_actor_engagement: Optional[float]
    time_idea_to_pr: Optional[float]
    time_pr_cycle_individual: Optional[float]
    time_ci_feedback: Optional[float]
    time_branch_lifetime: Optional[float]