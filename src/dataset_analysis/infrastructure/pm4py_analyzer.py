import statistics
import pm4py
import polars as pl
from typing import Optional, Any, Tuple, Dict
from pm4py.objects.log.obj import EventLog
from pm4py.objects.heuristics_net.obj import HeuristicsNet

from ..domain.interfaces import IProcessAnalyzer
from ..config import AnalysisConfig

# =============================================================================
# SEZIONE 1: LOG BUILDER (dal tuo file log_builder.py)
# Queste funzioni trasformano un DataFrame Polars in un EventLog PM4Py.
# =============================================================================

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

def build_behavior_actor_log(df: pl.DataFrame) -> Optional[EventLog]:
    """Funzione principale che orchestra la costruzione del log di eventi."""
    if df.is_empty():
        return None
    df_mapped = _map_columns(df)
    df_renamed = _normalize_event_names(df_mapped)
    df_final = _format_and_select_columns(df_renamed)
    df_sorted = df_final.sort(["case:concept:name", "time:timestamp"])
    return pm4py.convert_to_event_log(df_sorted.to_pandas())

# =============================================================================
# SEZIONE 2: KPI EXTRACTOR (dal tuo file kpi_extraction.py)
# Queste funzioni misurano il processo a partire dai modelli PM4Py.
# =============================================================================

def _median_cycle_time(log: EventLog, start_activity: str, end_activity: str) -> Optional[float]:
    """Calcola il tempo di ciclo mediano tra due attività all'interno di ogni traccia."""
    try:
        cycle_times = pm4py.stats.get_cycle_time(log, start_activity, end_activity)
        if not cycle_times:
            return None
        if isinstance(cycle_times, (int, float)):
            return float(cycle_times)
        return statistics.median(cycle_times)
    except Exception:
        return None

def extract_kpis(repo_id: str, strato_id: int, log: EventLog, heu_net: HeuristicsNet, performance_map: Dict) -> Dict[str, Any]:
    """Estrae tutti i KPI standardizzati per una repository."""
    nodes_map = getattr(heu_net, "nodes", {}) or {}
    dep_matrix = getattr(heu_net, "dependency_matrix", {}) or {}
    node_freq = getattr(heu_net, "node_frequencies", {}) or {}
    
    # Categoria A: Scala e Contesto
    num_attori = len(log) if log else 0
    num_eventi = sum(len(trace) for trace in log) if log else 0
    num_nodi = len(nodes_map)
    num_archi = len(dep_matrix)
    
    # Categoria B: Workflow e Formalità
    node_labels = list(nodes_map.keys())
    is_pr_based = 1 if "PullRequestEvent_opened" in node_labels else 0
    merged = node_freq.get("PullRequestEvent_merged", 0)
    rejected = node_freq.get("PullRequestEvent_rejected", 0)
    total_pr = merged + rejected
    pr_success_rate = merged / total_pr if total_pr > 0 else None
    uses_release = 1 if "ReleaseEvent_published" in node_labels else 0
    created = node_freq.get("CreateEvent_branch", 0)
    deleted = node_freq.get("DeleteEvent_branch", 0)
    branch_cleanup_rate = deleted / created if created > 0 else None
    
    # Categoria C: Collaborazione e Qualità
    dep_issue_open = dep_matrix.get(("IssuesEvent_opened", "CreateEvent_branch"), 0.0)
    dep_issue_comment = dep_matrix.get(("IssueCommentEvent_created", "CreateEvent_branch"), 0.0)
    is_issue_driven = 1 if (dep_issue_open > 0.1 or dep_issue_comment > 0.1) else 0
    rework_intensity = dep_matrix.get(("PullRequestReviewCommentEvent_created", "PullRequestEvent_synchronize"), 0.0)
    changes_req = node_freq.get("PullRequestReviewEvent_changes_requested", 0)
    approved = node_freq.get("PullRequestReviewEvent_approved", 0)
    total_review = changes_req + approved
    review_severity = changes_req / total_review if total_review > 0 else None
    
    # Categoria D: Performance e Tempi
    actor_engagement_times = pm4py.stats.get_all_case_durations(log, timestamp_key="time:timestamp") if log else []
    actor_engagement_time = statistics.median(actor_engagement_times) if actor_engagement_times else None
    idea_to_pr_time = performance_map.get(("CreateEvent_branch", "PullRequestEvent_opened"), None)
    pr_cycle_time_individual = _median_cycle_time(log, "PullRequestEvent_opened", "PullRequestEvent_merged")
    branch_lifetime = _median_cycle_time(log, "CreateEvent_branch", "DeleteEvent_branch")
    ci_candidates = [("PushEvent", "StatusEvent"), ("PushEvent", "CheckRunEvent")]
    possible_times = [performance_map.get(edge) for edge in ci_candidates] if performance_map else []
    valid_times = [t for t in possible_times if t is not None]
    time_ci_feedback = min(valid_times) if valid_times else None

    return {
        "repo_id": repo_id, "strato_id": strato_id, "num_eventi": num_eventi, "num_attori": num_attori,
        "num_nodi": num_nodi, "num_archi": num_archi, "is_pr_based": is_pr_based, "pr_success_rate": pr_success_rate,
        "uses_release": uses_release, "branch_cleanup_rate": branch_cleanup_rate, "is_issue_driven": is_issue_driven,
        "rework_intensity": rework_intensity, "review_severity": review_severity, "time_actor_engagement": actor_engagement_time,
        "time_idea_to_pr": idea_to_pr_time, "time_pr_cycle_individual": pr_cycle_time_individual,
        "time_ci_feedback": time_ci_feedback, "time_branch_lifetime": branch_lifetime,
    }

# =============================================================================
# SEZIONE 3: CLASSE ADATTATORE (Il "Wrapper" per PM4Py)
# =============================================================================

class PM4PyAnalyzer(IProcessAnalyzer):
    """
    Adattatore che implementa IProcessAnalyzer usando la libreria PM4Py.
    Incapsula la logica di costruzione del log, discovery e estrazione KPI.
    """
    def __init__(self, config: AnalysisConfig):
        self.config = config

    def analyze_repo(
        self,
        repo_lazy_frame: pl.LazyFrame,
        repo_id: str,
        strato_id: int
    ) -> Optional[Tuple[Dict[str, Any], Any, Any, EventLog]]:
        """
        Orchestra l'analisi per una singola repository.
        Restituisce una tupla con:
            - kpi_dict (Dict[str, Any])
            - modello di frequenza (HeuristicsNet)
            - event log (EventLog)
        """
        # 1️⃣ Rimuove eventi dei bot
        lf_humans_only = repo_lazy_frame.filter(
            ~pl.col("actor_login").str.ends_with("[bot]").fill_null(False)
        )

        # 2️⃣ Identifica attori "watcher passivi" (solo WatchEvent)
        watcher_ids_lf = (
            lf_humans_only
            .group_by("actor_id")
            .agg([
                (pl.col("activity").ne("WatchEvent").sum() == 0).alias("is_passive_watcher")
            ])
            .filter(pl.col("is_passive_watcher"))
            .select("actor_id")
        )

        # 3️⃣ Esclude eventi degli attori passivi (anti-join)
        lf_active_actors = lf_humans_only.join(watcher_ids_lf, on="actor_id", how="anti")

        # === [FASE 2] MATERIALIZZAZIONE MINIMA ===
        repo_df = repo_lazy_frame.collect(engine="streaming")

        # 1️⃣ Costruisci il log di eventi
        log = build_behavior_actor_log(repo_df)
        if not log or len(log) == 0:
            print(f"  -> [WARN] Log vuoto o non valido per repo {repo_id}, analisi saltata.")
            return None

        # 2️⃣ Scopri i modelli (Frequenza e Performance)
        try:
            # --- Modello di Frequenza (standard esplorativo) ---
            heu_net_freq = pm4py.discovery.discover_heuristics_net(log)

            # --- Modello di Performance ---
            heu_net_perf = pm4py.discovery.discover_heuristics_net(log, decoration="performance")

            # 🔍 Estrazione robusta della mappa di performance
            performance_dfg: Dict[Any, Any] = getattr(heu_net_perf, "performance_dfg", {}) or {}
            performance_map: Dict[Any, Any] = {edge: metrics for edge, metrics in performance_dfg.items()}

        except Exception as e:
            print(f"  -> [ERROR] Fallimento nella discovery del processo per repo {repo_id}: {e}")
            return None

        # 3️⃣ Estrai i KPI
        try:
            kpi_dict: Dict[str, Any] = extract_kpis(
                repo_id,
                strato_id,
                log,
                heu_net_freq,
                performance_map
            )
        except Exception as e:
            print(f"  -> [ERROR] Fallimento nell'estrazione KPI per repo {repo_id}: {e}")
            return None

        # 4️⃣ Restituisci tutti gli artefatti necessari alla pipeline
        return kpi_dict, heu_net_freq, heu_net_perf, log
    
    def analyze_repo_new(
        self,
        repo_lazy_frame: pl.LazyFrame, # L'input rimane un LazyFrame
        repo_id: str
    ) -> Optional[Tuple[Any, Any, EventLog]]:
        """
        Analizza una singola repository in modo efficiente, forzando l'ottimizzazione
        del predicate pushdown prima di applicare i filtri complessi.
        """
        print(f"\n[HeuristicMiner] 🔍 Analisi repo {repo_id} in corso...")

        try:
            # --- FASE 1: CARICAMENTO MIRATO FORZATO ---
            # Questo è il cuore della soluzione. Non applichiamo tutti i filtri subito.
            # Il 'repo_lazy_frame' che riceviamo contiene già il filtro per repo_id.
            
            # Eseguiamo la 'collect' QUI, su un piano lazy semplicissimo
            # (solo scan + filter su repo_id). Questo massimizza le possibilità
            # che Polars esegua il predicate pushdown correttamente.
            # 'streaming=True' è la nostra rete di sicurezza per la RAM.
            
            print(f"[HeuristicMiner] --> Avvio caricamento mirato da disco per {repo_id}...")
            df_repo_raw = repo_lazy_frame.collect(engine="streaming")
            print(f"[HeuristicMiner] --> Caricati {len(df_repo_raw)} eventi grezzi. Inizio filtraggio in memoria...")

            if df_repo_raw.is_empty():
                print(f"[WARN] Nessun evento trovato per repo {repo_id} durante il caricamento.")
                return None

            # --- FASE 2: FILTRAGGIO VELOCE IN MEMORIA ---
            # Ora che abbiamo un DataFrame in memoria (che dovrebbe essere piccolo
            # o gestibile in streaming), tutte le operazioni successive saranno veloci.

            # 1. Filtro Bot
            df_filtered = df_repo_raw.filter(
                ~pl.col("actor_login").str.ends_with("[bot]").fill_null(False)
            )

            # 2. Filtro per tipo di evento
            eventi_da_tenere = [
                "PushEvent", "IssuesEvent", "PullRequestEvent", "IssueCommentEvent",
                "PullRequestReviewCommentEvent", "PullRequestReviewEvent",
                "CommitCommentEvent", "CreateEvent"
            ]
            df_filtered = df_filtered.filter(pl.col("activity").is_in(eventi_da_tenere))

            print("pre filter")
            # 3. Filtro per azioni specifiche
            azioni_filter = (
                # 1. Tieni tutti i PushEvent
                (pl.col("activity") == "PushEvent") |

                # 2. Tieni solo le azioni di creazione/modifica per IssuesEvent
                (
                    (pl.col("activity") == "IssuesEvent") & 
                    (pl.col("action").is_in(["opened", "reopened", "edited"]))
                ) |

                # 3. Tieni le azioni di proposta/lavoro per PullRequestEvent
                (
                    (pl.col("activity") == "PullRequestEvent") & 
                    (pl.col("action").is_in(["opened", "reopened", "edited", "synchronize"]))
                ) |

                # 4. Tieni la creazione/modifica per tutti i tipi di commento
                (
                    (pl.col("activity") == "IssueCommentEvent") & 
                    (pl.col("action").is_in(["created", "edited"]))
                ) |
                (
                    (pl.col("activity") == "PullRequestReviewCommentEvent") & 
                    (pl.col("action").is_in(["created", "edited"]))
                ) |
                (
                    (pl.col("activity") == "CommitCommentEvent") & 
                    (pl.col("action") == "created")
                ) |

                # 5. Tieni solo la sottomissione di una review formale
                (
                    (pl.col("activity") == "PullRequestReviewEvent") & 
                    (pl.col("action") == "submitted") # Usa 'created' se nel tuo dataset l'azione si chiama così
                ) |

                # 6. Tieni solo la creazione di un branch
                (
                    (pl.col("activity") == "CreateEvent") & 
                    (pl.col("create_ref_type") == "branch")
                )
            )
        
            df_final = df_filtered.filter(azioni_filter)
            
            # -----------------------------------------------------------------
            # IL PUNTO IN CUI SI BLOCCAVA È STATO SUPERATO. ORA SIAMO IN RAM.
            # -----------------------------------------------------------------

            if df_final.is_empty():
                print(f"[WARN] Nessun evento rilevante trovato per repo {repo_id} dopo il filtraggio.")
                return None

            # --- FASE 3: PROCESS MINING (identica a prima) ---
            
            log = build_behavior_actor_log(df_final)
            if not log or len(log) == 0:
                print(f"[WARN] Log vuoto per repo {repo_id}.")
                return None
            
            print(f"[HeuristicMiner] 🧩 Log costruito: {len(log)} tracce, pronto per la discovery.")

            heu_net_freq = pm4py.discovery.discover_heuristics_net(log)
            heu_net_perf = pm4py.discovery.discover_heuristics_net(log, decoration="performance")
            print(f"[HeuristicMiner] ✅ Modelli scoperti per repo {repo_id}.")
            return heu_net_freq, heu_net_perf, log

        except Exception as e:
            print(f"[ERROR] ❌ Errore durante l’analisi di {repo_id}: {e}")
            return None


    def analyze_repo_process(self, event_log_lazy: pl.LazyFrame, repo_id: str) -> Optional[Dict[str, Any]]:
        """
        Esegue l'analisi di processo su un LazyFrame che si assume essere GIA'
        ottimizzato e pre-filtrato per una singola repository e senza bot.
        """
        print(f"[Analyzer] Avvio analisi di processo per repo_id: {repo_id}...")

        # 1. Materializza i dati. Ora questa operazione è sicura e veloce.
        print(f"[Analyzer] 🧠 Materializzazione degli eventi pre-filtrati per {repo_id}...")
        try:
            # Usiamo engine="streaming" come rete di sicurezza per repo molto grandi
            df_repo = event_log_lazy.collect(engine="streaming")
        except Exception as e:
            print(f"[Analyzer] ❌ Errore durante il caricamento per {repo_id}: {e}")
            return None

        if df_repo.is_empty():
            print(f"[Analyzer] ⚠️ Nessun evento trovato per {repo_id} dopo il caricamento.")
            return None

        print(f"[Analyzer] ✅ Caricati {len(df_repo)} eventi per {repo_id}.")

        # 2. Applica i filtri specifici del workflow (questo va bene farlo in memoria)
        filtered_df = self._filter_core_workflow_events_lazy(df_repo.lazy()).collect()

        if filtered_df.is_empty() or len(filtered_df) < 2:
            print(f"[Analyzer] ⚠️ Log per {repo_id} vuoto o insufficiente dopo i filtri core.")
            return None

        print(f"[Analyzer] ✨ Normalizzazione dei nomi degli eventi per {repo_id}...")
        try:
            # La funzione _normalize_event_names creerà la colonna 'concept:name'
            # che verrà usata da PM4Py come nome dell'attività.
            df_normalized = _normalize_event_names(filtered_df)
        except Exception as e:
            print(f"[Analyzer] ❌ ERRORE durante la normalizzazione degli eventi per {repo_id}: {e}")
            return None
        # =================================================================

        # 3. Conversione in formato PM4Py EventLog
        try:
            # Usiamo il nostro metodo di conversione aggiornato che sa come gestire
            # un DataFrame Polars già quasi pronto.
            log_pm4py = self._convert_df_to_pm4py_log(df_normalized)
        except Exception as e:
            print(f"[Analyzer] ❌ ERRORE nella conversione del log per {repo_id}: {e}")
            return None

        # 4. Heuristic Miner: discovery modelli Frequenza e Performance
        try:
            heu_net_freq = pm4py.discover_heuristics_net(log_pm4py)
            heu_net_perf = pm4py.discover_heuristics_net(log_pm4py, decoration="performance")
            print(f"[Analyzer] ✅ Modelli HeuristicsNet (Freq/Perf) generati per {repo_id}.")
            return {
                "frequency_model": heu_net_freq,
                "performance_model": heu_net_perf,
                "filtered_log": log_pm4py
            }
        except Exception as e:
            print(f"[Analyzer] ❌ ERRORE durante Heuristic Miner per {repo_id}: {e}")
            return None
    
    def _filter_core_workflow_events_lazy(self, log_df: pl.LazyFrame) -> pl.LazyFrame:
            """
            Filtra il log per mantenere solo gli eventi e le azioni "core"
            che sono gestiti dalla funzione di normalizzazione.
            Questo garantisce una coerenza totale tra filtraggio e analisi.
            """
            core_events_filter = (
                # 1. Eventi di base (sempre inclusi)
                (pl.col("activity") == "PushEvent") |
                
                # 2. Gestione delle Issue
                (
                    (pl.col("activity") == "IssuesEvent") & 
                    (pl.col("action").is_in(["opened", "reopened", "edited"]))
                ) |
                
                # 3. Gestione delle Pull Request (tutte le azioni rilevanti)
                (
                    (pl.col("activity") == "PullRequestEvent") & 
                    (pl.col("action").is_in(["opened", "reopened", "edited", "synchronize", "closed"]))
                ) |
                
                # 4. Gestione dei Commenti (tutti i tipi)
                (
                    (pl.col("activity") == "IssueCommentEvent") & 
                    (pl.col("action").is_in(["created", "edited"]))
                ) |
                (
                    (pl.col("activity") == "PullRequestReviewCommentEvent") & 
                    (pl.col("action").is_in(["created", "edited"]))
                ) |
                (
                    (pl.col("activity") == "CommitCommentEvent") & 
                    (pl.col("action") == "created")
                ) |
                
                # 5. Gestione delle Review Formali
                (
                    (pl.col("activity") == "PullRequestReviewEvent") & 
                    (pl.col("action") == "created")
                ) |
                
                # 6. Gestione della Creazione di Branch
                (
                    (pl.col("activity") == "CreateEvent") & 
                    (pl.col("create_ref_type") == "branch")
                )
            )
            return log_df.filter(core_events_filter)

    def _convert_df_to_pm4py_log(self, df: pl.DataFrame) -> Any:
        """
        Converte un DataFrame Polars (con eventi già normalizzati) 
        nel formato EventLog di PM4Py.
        """
        # 1. Mappa le colonne di base ('actor_id' -> 'case:concept:name')
        df_mapped = _map_columns(df)
        
        # 2. Seleziona le colonne finali. Questa funzione sa che 'concept:name'
        #    (il nome dell'attività) è già stato creato da _normalize_event_names.
        df_formatted = _format_and_select_columns(df_mapped)
        
        # 3. Ordina per coerenza
        df_sorted = df_formatted.sort(["case:concept:name", "time:timestamp"])
        
        # 4. Converti in pandas (requisito di PM4Py) e poi in EventLog
        return pm4py.convert_to_event_log(df_sorted.to_pandas()) 
