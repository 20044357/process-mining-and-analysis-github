import os
import json
from typing import Dict, Any
import pm4py
from pm4py.objects.log.obj import EventLog
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
import pyarrow as pa
import pyarrow.csv as pcsv
import polars as pl

class MetricsWriter:
    """
    Gestore per salvare metriche e grafici generati dalle analisi.
    Implementa una struttura di output scalabile basata sull'ID del repository.
    """

    def __init__(self, base_output: str = "data/dataset_analyzed"):
        self.base_output = base_output
        os.makedirs(self.base_output, exist_ok=True)

    def _get_repo_output_dir(self, repo_id: str) -> str:
        """
        Crea la struttura di directory per il repository specifico e restituisce il percorso base.
        """
        repo_dir = os.path.join(self.base_output, repo_id)
        os.makedirs(repo_dir, exist_ok=True)
        os.makedirs(os.path.join(repo_dir, "graphs"), exist_ok=True)
        os.makedirs(os.path.join(repo_dir, "metrics"), exist_ok=True)
        return repo_dir

    # ----------------------------------------
    # LOG (.xes)
    # ----------------------------------------
    def save_event_log(self, log: EventLog, strato_id: int, repo_id: str) -> str:
        """
        Salva il log XES dentro la stessa cartella dei grafi:
        /visuals/individual/strato_{strato_id}/{repo_id}/
        """
        base_dir = os.path.join(
            self.base_output, "visuals", "individual", f"strato_{strato_id}", repo_id
        )
        os.makedirs(base_dir, exist_ok=True)

        filename = f"{repo_id}_behavior_log.xes"
        out_path = os.path.join(base_dir, filename)

        try:
            xes_exporter.apply(log, out_path)
            print(f"[OK] Log di eventi salvato in {out_path}")
            return out_path
        except Exception as e:
            print(f"[ERROR] Impossibile salvare log XES per {repo_id}: {e}")
            return ""
    
    # ----------------------------------------
    # Polars -> CSV (.csv)
    # ----------------------------------------
    def save_polars_csv(self, df: pl.DataFrame, filename: str = "metrics.csv"):
        """
        Salva direttamente un DataFrame Polars in CSV,
        dentro la cartella base (globale).
        """
        output_path = os.path.join(self.base_output, filename)
        df.write_csv(output_path)
        print(f"[OK] CSV Polars salvato in {output_path}")


    # ----------------------------------------
    # GRAFICI (.png)
    # ----------------------------------------
    """def save_heuristics_net_graph(self, heu_net, repo_id: str, suffix: str = "") -> str:
        if heu_net is None:
            print(f"[WARN] Nessuna Heuristics Net per repo {repo_id}, grafico non generato.")
            return ""

        repo_dir = self._get_repo_output_dir(repo_id)
        graphs_dir = os.path.join(repo_dir, "graphs")
        filename_png = f"{repo_id}_heuristics{suffix}.png"
        out_path_png = os.path.join(graphs_dir, filename_png)

        pm4py.save_vis_heuristics_net(heu_net, out_path_png)
        print(f"[OK] Grafico Heuristics Net (.png) salvato per repo {repo_id} in {graphs_dir}")
        return out_path_png
    """


    def save_heuristics_net_graph(self, heu_net, repo_id: str, strato_id: int, suffix: str = "") -> str:
        if heu_net is None:
            print(f"[WARN] Nessuna Heuristics Net per repo {repo_id}, grafico non generato.")
            return ""

        base_dir = os.path.join(self.base_output, "visuals", "individual", f"strato_{strato_id}", repo_id)
        os.makedirs(base_dir, exist_ok=True)

        filename = f"{suffix}.png" if suffix else "graph.png"
        out_path = os.path.join(base_dir, filename)

        pm4py.save_vis_heuristics_net(heu_net, out_path)
        print(f"[OK] Grafo salvato: {out_path}")
        return out_path


    def save_dfg_graph(self, dfg_tuple, repo_id: str) -> str:
        if dfg_tuple is None:
            print(f"[WARN] Nessun DFG per repo {repo_id}, grafico non generato.")
            return ""

        dfg, start_activities, end_activities = dfg_tuple
        repo_dir = self._get_repo_output_dir(repo_id)
        graphs_dir = os.path.join(repo_dir, "graphs")
        filename_png = f"{repo_id}_dfg.png"
        out_path_png = os.path.join(graphs_dir, filename_png)

        pm4py.save_vis_dfg(dfg, start_activities, end_activities, out_path_png)
        print(f"[OK] Grafico DFG (.png) salvato per repo {repo_id} in {graphs_dir}")
        return out_path_png

    # ----------------------------------------
    # METRICHE / ANALISI (.json)
    # ----------------------------------------
    def _metrics_path(self, repo_id: str, filename: str) -> str:
        repo_dir = self._get_repo_output_dir(repo_id)
        metrics_dir = os.path.join(repo_dir, "metrics")
        return os.path.join(metrics_dir, filename)

    def save_dfg_metrics(self, dfg_data: Dict[str, Any], repo_id: str):
        out_path = self._metrics_path(repo_id, f"{repo_id}_process_metrics.json")
        try:
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(dfg_data, f, indent=4)
            print(f"[OK] Metriche DFG salvate in {out_path}")
        except Exception as e:
            print(f"[ERROR] Impossibile salvare metriche DFG per {repo_id}: {e}")

    def save_hm_metrics(self, repo_id: str, data: Dict[str, Any]):
        """
        Salva soglia scelta + complessità HM (nodi, archi).
        """
        out_path = self._metrics_path(repo_id, f"{repo_id}_hm_metrics.json")
        try:
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            print(f"[OK] Metriche HM salvate in {out_path}")
        except Exception as e:
            print(f"[ERROR] Impossibile salvare metriche HM per {repo_id}: {e}")

    def save_variants_analysis(self, repo_id: str, data: Dict[str, Any]):
        """
        Salva happy path, eccezioni e panoramica varianti.
        """
        out_path = self._metrics_path(repo_id, f"{repo_id}_variants_analysis.json")
        try:
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            print(f"[OK] Analisi varianti salvata in {out_path}")
        except Exception as e:
            print(f"[ERROR] Impossibile salvare analisi varianti per {repo_id}: {e}")

    def save_anti_patterns(self, repo_id: str, data: Dict[str, Any]):
        """
        Salva loop e ping-pong rilevati.
        """
        out_path = self._metrics_path(repo_id, f"{repo_id}_anti_patterns.json")
        try:
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            print(f"[OK] Anti-pattern salvati in {out_path}")
        except Exception as e:
            print(f"[ERROR] Impossibile salvare anti-pattern per {repo_id}: {e}")
