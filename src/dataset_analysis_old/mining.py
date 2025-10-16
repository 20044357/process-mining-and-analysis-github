import json
from typing import Dict, Any, Sequence, Set, Tuple, List, Optional, cast
import pandas as pd
import polars as pl
import pm4py
from pm4py.objects.log.obj import EventLog
from .utils import load_repo_strata
from .log_builder import build_behavior_actor_log
from .writers import MetricsWriter
from ..dataset_analysis.metrics.kpi_extraction import extract_kpis

# ------------------------------------------------------------
# 0) CALIBRAZIONE DELLA SOGLIA PER HEURISTIC MINER
# ------------------------------------------------------------
def calibrate_threshold(event_log: EventLog,
                        try_thresholds: Optional[List[float]] = None,
                        min_nodes: int = 2,
                        min_arcs: int = 1) -> float:
    """
    Cerca la soglia più alta che produce un modello 'sufficientemente ricco':
    - almeno 'min_nodes' nodi
    - almeno 'min_arcs' archi (dalla dependency_matrix)

    Strategia: tenta dall'alto verso il basso.
    """
    if try_thresholds is None:
        try_thresholds = [0.75, 0.60, 0.50, 0.40]

    for thr in try_thresholds:
        heu_net = pm4py.discover_heuristics_net(
            event_log,
            dependency_threshold=thr
        )
        if heu_net:
            nodes_ok = hasattr(heu_net, "nodes") and len(heu_net.nodes) >= min_nodes
            arcs_ok = False
            if hasattr(heu_net, "dependency_matrix") and isinstance(heu_net.dependency_matrix, dict):
                arc_count = sum(len(v) for v in heu_net.dependency_matrix.values())
                arcs_ok = arc_count >= min_arcs

            if nodes_ok and arcs_ok:
                return thr

    # fallback: accetta anche dipendenze deboli
    return 0.30


# ------------------------------------------------------------
# 1) DISCOVERY CON HM OTTIMIZZATO (MODELLO BASE)
# ------------------------------------------------------------
def discover_hm_optimized(event_log: EventLog) -> Tuple[Any, float, Dict[str, int]]:
    """
    - Calibra la soglia
    - Scopre il modello HM ottimizzato
    - Ritorna la rete, la soglia, e metriche di complessità
    """
    thr = calibrate_threshold(event_log)
    heu_net = pm4py.discover_heuristics_net(
        event_log,
        dependency_threshold=thr
    )

    metrics = {
        "nodes": len(heu_net.nodes) if heu_net and hasattr(heu_net, "nodes") else 0,
        "arcs": sum(len(v) for v in getattr(heu_net, "dependency_matrix", {}).values()) if heu_net else 0
    }

    return heu_net, thr, metrics

# ------------------------------------------------------------
# 2) HM "ALL" (parametri minimi) PER RIFLETTERE IL LOG GREZZO
# ------------------------------------------------------------
def discover_hm_all(event_log: EventLog) -> Any:
    """
    Manteniamo una rete 'all'
    """
    heu_net_all = pm4py.discover_heuristics_net(
        event_log,
        dependency_threshold=0.01,
        and_threshold=0.0,
        loop_two_threshold=0.0,
        min_act_count=1,
        min_dfg_occurrences=1
    )
    return heu_net_all

# ------------------------------------------------------------
# 2.1) HM "ALL" (parametri minimi) PER RIFLETTERE IL LOG GREZZO
# ------------------------------------------------------------
def discover_hm_default(event_log: EventLog) -> Any:
    """
    Manteniamo una rete con parametri 'default'
    """
    return pm4py.discover_heuristics_net(event_log)

def discover_hm_default_performance(event_log: EventLog) -> Any:
    """
    Manteniamo una rete con parametri 'default'
    """
    return pm4py.discover_heuristics_net(event_log, decoration="performance")

# ------------------------------------------------------------
# 3) ANALISI: HAPPY PATH & ECCEZIONI
# ------------------------------------------------------------
from typing import Dict, Any, List
from pm4py.objects.log.obj import EventLog

from typing import Dict, Any
from pm4py.objects.log.obj import EventLog
from pm4py.statistics.variants.log import get as variants_get

def analyze_variants_with_traces(event_log: EventLog,
                                 exceptions_ratio: float = 0.05) -> Dict[str, Any]:
    """
    Versione avanzata: ritorna anche le tracce reali che compongono happy path ed eccezioni.
    """
    variants_dict = variants_get.get_variants(event_log)  # { "A,B,C": [trace1, trace2, ...], ... }
    num_traces = len(event_log)

    # Costruisci lista di statistiche
    variant_stats = [{"variant": var, "count": len(traces), "traces": [t.attributes.get("concept:name", f"case_{i}") for i, t in enumerate(traces)]}
                     for var, traces in variants_dict.items()]

    variant_stats = sorted(variant_stats, key=lambda x: x["count"], reverse=True)

    happy_path = variant_stats[0] if variant_stats else None
    exceptions = [v for v in variant_stats if v["count"] < exceptions_ratio * num_traces]

    overview = {
        "num_traces": num_traces,
        "num_variants": len(variant_stats),
        "top5_variants": [
            {"variant": v["variant"], "count": v["count"]} for v in variant_stats[:5]
        ]
    }

    return {
        "happy_path": happy_path,
        "exceptions": exceptions,
        "overview": overview
    }


from typing import Dict, Any, List
from pm4py.objects.log.obj import EventLog

def detect_anti_patterns(event_log: EventLog,
                         min_support: int = 2) -> Dict[str, Any]:
    """
    Rileva anti-pattern:
    - Loop breve A->B->A (3-step)
    - Ping-pong A->B->A->B (4-step)

    Funziona sia se get_variant_statistics ritorna dict che tuple.
    """
    from pm4py.statistics.traces.generic.log import case_statistics

    variant_stats: List[Any] = case_statistics.get_variant_statistics(event_log)

    loops: List[Dict[str, Any]] = []
    pingpongs: List[Dict[str, Any]] = []

    for v in variant_stats:
        # Caso 1: v è un dict (forma standard in PM4Py >= 2.7)
        if isinstance(v, dict):
            variant = v.get("variant", "")
            cnt = v.get("count", 0)
        # Caso 2: v è una tupla (variant, count) → fallback per altre versioni
        elif isinstance(v, tuple) and len(v) == 2:
            variant, cnt = v
        else:
            continue

        seq = str(variant).split(",")
        if cnt < min_support:
            continue

        # Loop A-B-A
        for i in range(len(seq) - 2):
            if seq[i] == seq[i + 2]:
                loops.append({
                    "pattern": [seq[i], seq[i + 1], seq[i + 2]],
                    "support": cnt
                })

        # Ping-pong A-B-A-B
        for i in range(len(seq) - 3):
            if seq[i] == seq[i + 2] and seq[i + 1] == seq[i + 3] and seq[i] != seq[i + 1]:
                pingpongs.append({
                    "pattern": [seq[i], seq[i + 1], seq[i + 2], seq[i + 3]],
                    "support": cnt
                })

    return {
        "loops_3step_ABA": loops,
        "pingpong_4step_ABAB": pingpongs
    }



# ------------------------------------------------------------
# 5) DFG (opzionale, riciclo del tuo codice per metriche)
# ------------------------------------------------------------
def discover_dfg(log: EventLog) -> Tuple[Dict, Dict, Dict]:
    return pm4py.discover_dfg(log)


def extract_dfg_metrics(dfg_tuple: Tuple[Dict, Dict, Dict], log: EventLog) -> Dict[str, Any]:
    dfg, start_activities, end_activities = dfg_tuple
    activities_frequencies = pm4py.get_event_attribute_values(log, "concept:name")
    dfg_for_json = {f"{src[0]} -> {src[1]}": count for src, count in dfg.items()}

    return {
        "activities_frequencies": activities_frequencies,
        "dfg_transition_counts": dfg_for_json,
        "start_activities": list(start_activities.keys()),
        "end_activities": list(end_activities.keys()),
    }


# ------------------------------------------------------------
# 6) ORCHESTRATORE
# ------------------------------------------------------------
def run_mining(base_dir: str, sample_df: pl.DataFrame):
    writer = MetricsWriter()
    lf_all = pl.scan_parquet(f"{base_dir}/**/*.parquet")
    
    results = []

    # Carica la mappa degli strati
    for row in sample_df.iter_rows(named=True):
        repo_id = str(row["repo_id"])
        strato_id = int(row["strato_id"])

        # --- Costruisco e Controllo validità log della repo x
        df_repo = lf_all.filter(pl.col("repo_id").cast(pl.Utf8) == repo_id).collect()
        behavior_actor_log = build_behavior_actor_log(df_repo)
        if behavior_actor_log is None or len(behavior_actor_log) == 0:
            print(f"[WARN] Repo {repo_id}: log non valido o vuoto, salto.")
            continue

        """
        # --- DISCOVERY: HM ottimizzato ---
        hm_opt, thr, hm_opt_metrics = discover_hm_optimized(behavior_actor_log)
        print(f"HM OPTMIZED: {hm_opt}")

        used_model_type = None
        if hm_opt and hm_opt_metrics["nodes"] > 1:
            used_model_type = "optimized"
        else:
            used_model_type = "all_fallback"
        """

        # ---------------------------
        #   DISCOVERY 
        # ---------------------------
        
        # --- DISCOVERY: HM 'all' (grezzo) ---
        hm_all = discover_hm_all(behavior_actor_log)
        
        # --- DISCOVERY: HM 'default' (frq e performance) ---
        hm_default = discover_hm_default(behavior_actor_log)
        hm_default_performance = discover_hm_default_performance(behavior_actor_log)
        performance_dfg = getattr(hm_default_performance, "performance_dfg", {}) or {}
        performance_map_dict = {edge: metrics for edge, metrics in performance_dfg.items()}
        # ---------------------------
        #   ANALISI 
        # ---------------------------
        """
        variants_info = analyze_variants_with_traces(behavior_actor_log)
        anti_patterns = detect_anti_patterns(behavior_actor_log)
        # --- (opz) DFG & metriche ---
        dfg_tuple = discover_dfg(behavior_actor_log)
        dfg = dfg_tuple[0] if dfg_tuple else {}
        dfg_metrics = extract_dfg_metrics(dfg_tuple, behavior_actor_log) if dfg_tuple else {}
        """
        # --- KPI ---
        kpis = extract_kpis(repo_id, strato_id, behavior_actor_log, hm_default, performance_map_dict)
        results.append(kpis)
        print(kpis)
        print("\n")
        
        # ---------------------------
        #   SALVATAGGI
        # ---------------------------
        """
        # Grafico HM ottimizzato (solo se valido)
        if hm_opt and hm_opt_metrics["nodes"] > 1:
            writer.save_heuristics_net_graph(hm_opt, repo_id, suffix="_hm_optimized")
        else:
            print(f"[SKIP] Repo {repo_id}: heuristics net ottimizzata vuota/povera, grafico non generato.")
        """

        # --- LOG (xes) ----
        writer.save_event_log(behavior_actor_log, strato_id, repo_id)

        # --- Grafico HM all (grezzo) ---
        if hm_all and hasattr(hm_all, "nodes") and len(hm_all.nodes) > 0:
            writer.save_heuristics_net_graph(hm_all, repo_id, strato_id, suffix="_hm_all")
        else:
            print(f"[SKIP] Repo {repo_id}: heuristics net 'all' vuota, grafico non generato.")

        # --- Grafico HM default (frq) ---
        if hm_default and hasattr(hm_default, "nodes") and len(hm_default.nodes) > 0:
            writer.save_heuristics_net_graph(hm_default, repo_id, strato_id, suffix="_hm_default_frq")
        else:
            print(f"[SKIP] Repo {repo_id}: heuristics net 'all' vuota, grafico non generato.")

        # Grafico HM default (performance)
        if hm_default_performance and hasattr(hm_default_performance, "nodes") and len(hm_default_performance.nodes) > 0:
            writer.save_heuristics_net_graph(hm_default_performance, repo_id, strato_id, suffix="_hm_default_performance")
        else:
            print(f"[SKIP] Repo {repo_id}: heuristics net 'all' vuota, grafico non generato.")

        # Analisi varianti
        #writer.save_variants_analysis(repo_id=repo_id, data=variants_info)

        # Anti-pattern
        #writer.save_anti_patterns(repo_id=repo_id, data=anti_patterns)

    df_results = pd.DataFrame(results)
    df_results.to_csv("kpi_results.csv", index=False)
    print(f"[OK] Tabella KPI salvata in kpi_results.csv ({len(df_results)} repo)")

    return df_results

