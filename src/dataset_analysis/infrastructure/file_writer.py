# File: src/dataset_analysis/infrastructure/file_writer.py

import os
import json
import pickle
import pandas as pd
import polars as pl
import pm4py
from pm4py.objects.log.obj import EventLog
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from typing import Dict, Any, List

from ..domain.interfaces import IResultWriter
from ..config import AnalysisConfig

class FileResultWriter(IResultWriter):
    """
    Adattatore che implementa IResultWriter per salvare i risultati su file system locale.
    È una versione ristrutturata della tua vecchia classe MetricsWriter.
    """
    def __init__(self, config: AnalysisConfig):
        self.config = config
        os.makedirs(self.config.analysis_base_dir, exist_ok=True)
        print("[Writer] Inizializzato. Salverà i risultati in:", self.config.analysis_base_dir)

    def save_entities_as_dataframe(self, entities: List, filename: str):
        if not entities: return
        # Converte la lista di dataclass in un DataFrame
        list_of_dicts = [e.__dict__ for e in entities]
        df_to_save = pl.DataFrame(list_of_dicts)
        self.save_dataframe(df_to_save, filename) # Riutilizza il metodo esistente

    def save_lazyframe(self, lf: pl.LazyFrame, filename: str):
        """Salva un LazyFrame direttamente su disco in modalità streaming (senza collect)."""
        output_path = os.path.join(self.config.analysis_base_dir, filename)
        try:
            lf.sink_parquet(output_path)
            print(f"[Writer] LazyFrame salvato in streaming su: {output_path}")
        except Exception as e:
            print(f"[ERROR] Impossibile salvare LazyFrame su {output_path}: {e}")

    def save_polars_csv(self, df: pl.DataFrame, filename: str):
        """
        Salva direttamente un DataFrame Polars in CSV,
        dentro la cartella base (globale).
        """
        output_path = os.path.join(self.config.analysis_base_dir, filename)
        df.write_csv(output_path)
        print(f"[OK] CSV Polars salvato in {output_path}")

    def save_dataframe(self, df: pl.DataFrame, filename: str):
        """Salva un DataFrame Polars in un file (CSV o Parquet)."""
        output_path = os.path.join(self.config.analysis_base_dir, filename)
        if filename.endswith(".parquet"):
            df.write_parquet(output_path)
        else:
            df.write_csv(output_path)
        print(f"[Writer] DataFrame salvato in: {output_path}")

    def save_json(self, data: Dict, filename: str):
        """Salva un dizionario in un file JSON."""
        output_path = os.path.join(self.config.analysis_base_dir, filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        print(f"[Writer] JSON salvato in: {output_path}")

    def save_id_list(self, ids: List[str], filename: str):
        """Salva una lista di ID in un file di testo."""
        output_path = os.path.join(self.config.analysis_base_dir, filename)
        with open(output_path, 'w') as f:
            for item_id in ids:
                f.write(f"{item_id}\n")
        print(f"[Writer] Lista ID salvata in: {output_path}")

    def _get_individual_output_dir(self, repo_id: str, strato_id: int) -> str:
        """Helper per creare la cartella di output per una singola repo."""
        repo_dir = os.path.join(
            self.config.analysis_base_dir, "visuals", "individual", f"strato_{strato_id}", repo_id
        )
        os.makedirs(repo_dir, exist_ok=True)
        return repo_dir

    def save_log_xes(self, log: EventLog, repo_id: str, strato_id: int):
        """Salva un EventLog in formato XES."""
        if not log:
            return
        output_dir = self._get_individual_output_dir(repo_id, strato_id)
        output_path = os.path.join(output_dir, f"{repo_id}_log.xes")
        try:
            xes_exporter.apply(log, output_path)
            # print(f"[Writer] Log XES salvato in: {output_path}") # Opzionale, per ridurre il verbosità
        except Exception as e:
            print(f"[ERROR] Impossibile salvare log XES per {repo_id}: {e}")

    def save_heuristics_net(self, heu_net: Any, repo_id: str, strato_id: int, suffix: str):
        """Salva un'immagine del modello Heuristics Net."""
        if not heu_net or not hasattr(heu_net, "nodes") or not heu_net.nodes:
            # print(f"[WARN] Heuristics Net vuoto per {repo_id}, grafico non generato.")
            return
        
        output_dir = self._get_individual_output_dir(repo_id, strato_id)
        output_path = os.path.join(output_dir, f"{suffix}.png")
        try:
            pm4py.save_vis_heuristics_net(heu_net, output_path)
            # print(f"[Writer] Grafo salvato in: {output_path}")
        except Exception as e:
            print(f"[ERROR] Impossibile salvare grafo per {repo_id}: {e}")

    def save_kpis(self, kpis: List[Dict[str, Any]]):
        """Salva la tabella finale dei KPI in formato CSV."""
        if not kpis:
            print("[WARN] Nessun KPI da salvare.")
            return
        
        # pm4py talvolta usa tipi numpy che non sono serializzabili in modo standard,
        # quindi convertire a DataFrame pandas è più sicuro.
        df_results = pd.DataFrame(kpis)
        df_results.to_csv(self.config.kpi_results_path, index=False)
        print(f"[Writer] Tabella KPI salvata in: {self.config.kpi_results_path}")

    def save_analysis_artifacts(self, results: Dict[str, Any], repo_id: str, archetype_name: str, suffix: str = ""):
        # ... (la logica di creazione della directory rimane la stessa, con la correzione per UnboundLocalError) ...
        output_dir = ""
        try:
            safe_archetype_name = "".join(c for c in archetype_name if c.isalnum() or c in ('_', '-')).rstrip()
            output_dir = os.path.join(self.config.analysis_base_dir, "process_analysis", safe_archetype_name, str(repo_id))
            os.makedirs(output_dir, exist_ok=True)
        except Exception as e:
            print(f"[Writer] ❌ ERRORE CRITICO nella creazione della directory {output_dir}: {e}")
            return

        # 2. Estrai gli oggetti dal dizionario dei risultati
        freq_model = results.get("frequency_model")
        perf_model = results.get("performance_model")
        log_pm4py = results.get("filtered_log")
        
        suffix_str = f"_{suffix}" if suffix else ""

        # --- LOGICA DI SALVATAGGIO FINALE E CORRETTA ---

        # 3. Salva la mappa di frequenza
        if freq_model:
            try:
                path = os.path.join(output_dir, f"frequency_map{suffix_str}.png")
                # Ora passiamo direttamente l'oggetto, come giustamente indicato da te
                pm4py.save_vis_heuristics_net(freq_model, path, graph_title=f"Frequency - {repo_id}")
                print(f"[Writer] ✅ Mappa di frequenza salvata in: {path}")
            except Exception as e:
                print(f"[Writer] ❌ ERRORE durante il salvataggio della mappa di frequenza per {repo_id}: {e}")
        
        # 4. Salva la mappa di performance
        if perf_model:
            try:
                path = os.path.join(output_dir, f"performance_map{suffix_str}.png")
                # Passiamo direttamente anche questo oggetto
                pm4py.save_vis_heuristics_net(perf_model, path, graph_title=f"Performance - {repo_id}")
                print(f"[Writer] ✅ Mappa di performance salvata in: {path}")
            except Exception as e:
                print(f"[Writer] ❌ ERRORE durante il salvataggio della mappa di performance per {repo_id}: {e}")
        # 5. Salva il log filtrato in formato XES
        if log_pm4py:
            try:
                path = os.path.join(output_dir, f"filtered_log{suffix_str}.xes")
                xes_exporter.apply(log_pm4py, path)
                print(f"[Writer] ✅ Log XES salvato in: {path}")
            except Exception as e:
                print(f"[Writer] ❌ ERRORE durante il salvataggio del log XES per {repo_id}: {e}")

        # Salva l'oggetto HeuristicsNet della frequenza
        if freq_model:
            try:
                path = os.path.join(output_dir, f"frequency_model{suffix_str}.pkl")
                with open(path, 'wb') as f:
                    pickle.dump(freq_model, f)
                print(f"[Writer] ✅ Modello di frequenza (PKL) salvato.")
            except Exception as e:
                print(f"[Writer] ❌ ERRORE durante il salvataggio del pickle di frequenza per {repo_id}: {e}")

        # Salva l'oggetto HeuristicsNet della performance
        if perf_model:
            try:
                path = os.path.join(output_dir, f"performance_model{suffix_str}.pkl")
                with open(path, 'wb') as f:
                    pickle.dump(perf_model, f)
                print(f"[Writer] ✅ Modello di performance (PKL) salvato.")
            except Exception as e:
                print(f"[Writer] ❌ ERRORE durante il salvataggio del pickle di performance per {repo_id}: {e}")

    def save_quantitative_summary(self, summary_df: pl.DataFrame, filename: str):
        """Salva il DataFrame di sintesi dei KPI in formato CSV."""
        path = os.path.join(self.config.analysis_base_dir, filename)
        try:
            summary_df.write_csv(path)
            print(f"[Writer] ✅ Tabella di sintesi quantitativa salvata in: {path}")
        except Exception as e:
            print(f"[Writer] ❌ ERRORE durante il salvataggio della sintesi quantitativa: {e}")