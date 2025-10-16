import statistics
import pm4py
from pm4py.objects.log.obj import EventLog
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from typing import Dict, Any

# ------------------------------
# Helper per tempi di ciclo
# ------------------------------
def _median_cycle_time(log: EventLog, start_activity: str, end_activity: str) -> float | None:
    """
    Calcola il tempo di ciclo mediano tra due attività ALL'INTERNO di ogni traccia (attore).
    Utile per misurare durate di processi completati da un singolo individuo.
    """
    try:
        cycle_times = pm4py.stats.get_cycle_time(
            log,
            start_activity,
            end_activity
        )
        if not cycle_times:
            return None
        if isinstance(cycle_times, (int, float)):
            return float(cycle_times)
        return statistics.median(cycle_times)
    except Exception:
        return None

# ------------------------------
# Funzione principale
# ------------------------------
def extract_kpis(repo_id: str, strato_id: int, log: EventLog, heu_net: HeuristicsNet, performance_map: Dict) -> Dict[str, Any]:
    """
    Estrae tutti i KPI standardizzati per una repository, accedendo in modo
    robusto agli attributi dell'oggetto HeuristicsNet.

    Parametri
    ---------
    repo_id : str
        Identificativo della repository.
    strato_id : int
        Archetipo (1..8) della repository.
    log : EventLog
        Log PM4Py degli eventi della repo (case_id = actor_id).
    heu_net : HeuristicsNet
        Modello di processo (frequenza) scoperto con Heuristic Miner.
    performance_map : dict
        Mappa delle performance {arco: tempo_medio}.

    Ritorna
    -------
    dict : KPI calcolati.
    """
    
    # --- Preparazione delle fonti di verità dall'oggetto heu_net ---
    nodes_map = getattr(heu_net, "nodes", {}) or {}
    dep_matrix = getattr(heu_net, "dependency_matrix", {}) or {}
    node_freq = getattr(heu_net, "node_frequencies", {}) or {}

    # ----------------------
    # Categoria A: Scala e Contesto
    # ----------------------
    
    # Significato: Il numero di attori unici (casi) nel log. La dimensione della "popolazione".
    num_attori = len(log) if log else 0
    
    # Significato: Il volume totale di azioni individuali compiute da tutti gli attori.
    num_eventi = sum(len(trace) for trace in log) if log else 0

    # Significato: La varietà di azioni diverse nel repertorio comportamentale degli attori.
    num_nodi = len(nodes_map)
    
    # Significato: L'interconnessione tra i comportamenti. Misura la complessità delle sequenze di azioni.
    num_archi = len(dep_matrix)

    # ----------------------
    # Categoria B: Workflow e Formalità
    # ----------------------
    node_labels = list(nodes_map.keys())

    # Significato: (1/0) Indica se 'aprire una PR' è un comportamento presente nel workflow degli attori.
    is_pr_based = 1 if "PullRequestEvent_opened" in node_labels else 0

    # Significato: Percentuale di azioni di chiusura PR che sono 'merge'. Misura la prevalenza del successo.
    merged = node_freq.get("PullRequestEvent_merged", 0)
    rejected = node_freq.get("PullRequestEvent_rejected", 0)
    total_pr = merged + rejected
    pr_success_rate = merged / total_pr if total_pr > 0 else None

    # Significato: (1/0) Indica se 'pubblicare una release' è un'azione compiuta dagli attori, segno di maturità.
    uses_release = 1 if "ReleaseEvent_published" in node_labels else 0

    # Significato: Rapporto tra branch cancellati e creati. Misura la 'disciplina' o 'igiene' del progetto.
    created = node_freq.get("CreateEvent_branch", 0)
    deleted = node_freq.get("DeleteEvent_branch", 0)
    branch_cleanup_rate = deleted / created if created > 0 else None

    # ----------------------
    # Categoria C: Collaborazione e Qualità (Pattern Sociali)
    # ----------------------

    # Significato: (1/0) Indica se esiste un pattern comportamentale dove un'interazione con una Issue porta a codice.
    dep_issue_open = dep_matrix.get(("IssuesEvent_opened", "CreateEvent_branch"), 0.0)
    dep_issue_comment = dep_matrix.get(("IssueCommentEvent_created", "CreateEvent_branch"), 0.0)
    is_issue_driven = 1 if (dep_issue_open > 0.1 or dep_issue_comment > 0.1) else 0

    # Significato: Forza del pattern 'ricevo feedback -> agisco'. Misura la cultura di reattività alla revisione.
    rework_intensity = dep_matrix.get(
        ("PullRequestReviewCommentEvent_created", "PullRequestEvent_synchronize"), 0.0
    )

    # Significato: Percentuale di revisioni formali che sono 'rosse'. Misura la prevalenza di un atteggiamento critico.
    changes_req = node_freq.get("PullRequestReviewEvent_changes_requested", 0)
    approved = node_freq.get("PullRequestReviewEvent_approved", 0)
    total_review = changes_req + approved
    review_severity = changes_req / total_review if total_review > 0 else None

    # ----------------------
    # Categoria D: Performance e Tempi (Cadenza Individuale)
    # ----------------------
    
    # Significato: Durata mediana dell'engagement di un attore, dal suo primo al suo ultimo evento.
    actor_engagement_times = pm4py.stats.get_all_case_durations(log, timestamp_key="time:timestamp") if log else []
    actor_engagement_time = statistics.median(actor_engagement_times) if actor_engagement_times else None

    # Significato: Tempo mediano che un attore impiega tra la creazione di un branch e l'apertura di una PR.
    idea_to_pr_time = performance_map.get(("CreateEvent_branch", "PullRequestEvent_opened"), None)

    # Significato: (Approssimato) Tempo mediano per i cicli di PR completati da un SINGOLO attore.
    pr_cycle_time_individual = _median_cycle_time(log, "PullRequestEvent_opened", "PullRequestEvent_merged")
    
    # Significato: Durata di vita mediana dei branch creati e cancellati dallo STESSO attore.
    branch_lifetime = _median_cycle_time(log, "CreateEvent_branch", "DeleteEvent_branch")

    # Significato: Tempo mediano che un attore attende per il feedback della CI dopo un SUO push.
    ci_candidates = [("PushEvent", "StatusEvent"), ("PushEvent", "CheckRunEvent")]
    possible_times = [performance_map.get(edge) for edge in ci_candidates] if performance_map else []
    valid_times = [t for t in possible_times if t is not None]
    time_ci_feedback = min(valid_times) if valid_times else None

    # ----------------------
    # Output finale
    # ----------------------
    return {
        # Metadata
        "repo_id": repo_id,
        "strato_id": strato_id,
        
        # Categoria A
        "num_eventi": num_eventi,
        "num_attori": num_attori,
        "num_nodi": num_nodi,
        "num_archi": num_archi,
        
        # Categoria B
        "is_pr_based": is_pr_based,
        "pr_success_rate": pr_success_rate,
        "uses_release": uses_release,
        "branch_cleanup_rate": branch_cleanup_rate,
        "is_issue_driven": is_issue_driven,
        
        # Categoria C
        "rework_intensity": rework_intensity,
        "review_severity": review_severity,
        
        # Categoria D
        "time_actor_engagement": actor_engagement_time,
        "time_idea_to_pr": idea_to_pr_time,
        "time_pr_cycle_individual": pr_cycle_time_individual,
        "time_ci_feedback": time_ci_feedback,
        "time_branch_lifetime": branch_lifetime,
    }