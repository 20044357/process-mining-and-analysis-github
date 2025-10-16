from enum import Enum
import gc
import os
from pathlib import Path
import pandas as pd
import pm4py
import polars as pl
from typing import List, Dict, Any
from pm4py.objects.log.exporter.xes import exporter as xes_exporter

from ..config import AnalysisConfig
from ..domain.interfaces import IDataProvider, IProcessAnalyzer, IResultWriter, IModelAnalyzer
from ..domain import services as domain_services
from ..domain.entities import RepositoryMetrics

class AnalysisMode(Enum):
    FULL_PIPELINE = "full"
    GIGANTI_POPOLARI = "giganti_popolari"
    GIGANTI_NO_POPOLARI = "giganti_no_popolari"
    GIGANTI_POPOLARI_NO_COLLAB = "giganti_popolari_no_collab"
    Q1_TOXIC_VS_HEALTHY = "q1_toxic"
    Q2_HYPE_IMPACT = "q2_hype"
    Q3_REVIEW_BIRTH = "q3_review"
    SUMMARIZE_RESULTS = "summarize"

class AnalysisPipeline:
    """
    Orchestra l'intera pipeline di analisi, dal caricamento dati al salvataggio dei KPI.
    Implementa la logica di alto livello del processo di analisi.
    """
    def __init__(self, provider: IDataProvider, analyzer: IProcessAnalyzer, writer: IResultWriter, config: AnalysisConfig, mode_analyzer: IModelAnalyzer):
        # Iniezione delle dipendenze: la pipeline riceve i suoi "strumenti" dall'esterno.
        self.provider = provider
        self.analyzer = analyzer
        self.writer = writer
        self.model_analyzer = mode_analyzer
        self.config = config
        print("[Pipeline] Pipeline di analisi inizializzata.")

    def run(self, mode: AnalysisMode):
        """Punto di ingresso principale che smista l'esecuzione in base alla modalità."""
        print(f"🚀 Avvio pipeline in modalità: {mode}")

        if mode == AnalysisMode.FULL_PIPELINE:
            self._run_full_data_prep_pipeline()
        elif mode == AnalysisMode.GIGANTI_POPOLARI:
            self._run_giganti_collab_popolari()
        elif mode == AnalysisMode.GIGANTI_NO_POPOLARI:
            self._run_giganti_collab_no_popolari()
        elif mode == AnalysisMode.GIGANTI_POPOLARI_NO_COLLAB:
            self._run_giganti_no_collab_popolari()
        #elif mode == AnalysisMode.Q1_TOXIC_VS_HEALTHY:
            #self._run_q1_analysis()
        #elif mode == AnalysisMode.Q2_HYPE_IMPACT:
            #self._run_q2_analysis()
        #elif mode == AnalysisMode.Q3_REVIEW_BIRTH:
            #self._run_q3_analysis()
        elif mode == AnalysisMode.SUMMARIZE_RESULTS:
            self._run_summary_analysis()
        else:
            raise ValueError(f"Modalità '{mode}' non supportata.")

        print(f"✅ Pipeline in modalità '{mode}' completata con successo.")


    # =========================================================================
    # --- METODO PER LA PREPARAZIONE COMPLETA DEI DATI (MODALITÀ "full") ---
    # =========================================================================
    def _run_full_data_prep_pipeline(self):
        """Esegue l'intero flusso di preparazione dati: calcolo metriche, stratificazione e sintesi."""
        
        # --- Fase 1: Calcolo Metriche ---
        print("\n--- [FASE 1/4] Calcolo Metriche 'Golden Recipe' ---")
        eligible_ids = self.provider.get_eligible_repo_ids()
        metrics_df = self.provider.calculate_summary_metrics(eligible_ids)
        self.writer.save_lazyframe(metrics_df, Path(self.config.metrics_file_path).name)

        # --- Fase 2: Stratificazione ---
        print("\n--- [FASE 2/4] Stratificazione a Quantili ---")
        stratified_df, thresholds = domain_services.stratify(self.config.metrics_file_path, config=self.config)
        self.writer.save_dataframe(stratified_df, Path(self.config.stratified_file_path_parquet).name)
        self.writer.save_json(thresholds, Path(self.config.thresholds_file_path).name)

        # --- Fase 3: Distribuzione degli Strati ---
        print("\n--- [FASE 3/4] Analisi della Distribuzione degli Strati ---")
        counts_df = domain_services.count_by_stratum(stratified_df)
        self.writer.save_dataframe(counts_df, Path(self.config.strata_distribution_file_path_csv).name)

        # --- Fase 4: Sintesi Archetipica ---
        """print("\n--- [FASE 4/4] Generazione Sintesi Archetipica ---")
        summary_df = domain_services.summarize_archetypes(
            strata_distribution_path=os.path.join(self.config.analysis_base_dir, "strata_distribution.csv"),
            config=self.config,
            output_path=os.path.join(self.config.analysis_base_dir, "archetype_summary.csv")
        )"""

    # =========================================================================
    # --- METODI PER LE ANALISI DI PROCESSO MIRATE ---
    # =========================================================================
    def _run_giganti_collab_popolari(self):
        print("\n--- [Q1] Avvio Analisi: Collaborazione 'Sana' vs 'Inefficace' ---")
        
        # 1. Carica il file stratificato (l'unica dipendenza)
        try:
            stratified_df = self.provider.get_stratified_data()
        except Exception as e:
            print(f"❌ ERRORE: Impossibile caricare il file stratificato. Dettagli: {e}")
            return

        # 2. Definizione archetipi (modificata)
        profiles_to_analyze = {
            "Giganti Collaborativi Popolari": (
                (pl.col("popolarita_esterna_norm_cat").is_in(["Gigante"])) &
                (pl.col("intensita_collaborativa_norm_cat").is_in(["Gigante"])) &
                (pl.col("volume_lavoro_norm_cat").is_in(["Gigante"])) &
                (pl.col("engagement_community_norm_cat").is_in(["Gigante"])) #Alto
            )
        }
        
        # 3. Seleziona i campioni usando i profili definiti
        sampled_repos = domain_services.select_representative_repos(
            stratified_df=stratified_df,
            config=self.config,
            archetype_profiles=profiles_to_analyze,
            sampling_metric="intensita_collaborativa_cum"
        )
        
        # 4. Esegui l'analisi
        self._execute_process_analysis_loop(sampled_repos)

    def _run_giganti_collab_no_popolari(self):
        print("\n--- [Q1] Avvio Analisi: Collaborazione 'Sana' vs 'Inefficace' ---")
        
        # 1. Carica il file stratificato (l'unica dipendenza)
        try:
            stratified_df = self.provider.get_stratified_data()
        except Exception as e:
            print(f"❌ ERRORE: Impossibile caricare il file stratificato. Dettagli: {e}")
            return

        # 2. Definizione archetipi (modificata)
        profiles_to_analyze = {
            "Giganti Collaborativi Non Popolari": (
                (pl.col("popolarita_esterna_norm_cat").is_in(["Zero"])) &
                (pl.col("intensita_collaborativa_norm_cat").is_in(["Gigante"])) &
                (pl.col("volume_lavoro_norm_cat").is_in(["Gigante"])) &
                (pl.col("engagement_community_norm_cat").is_in(["Zero"]))
            )
        }
        
        # 3. Seleziona i campioni usando i profili definiti
        sampled_repos = domain_services.select_representative_repos(
            stratified_df=stratified_df,
            config=self.config,
            archetype_profiles=profiles_to_analyze,
            sampling_metric="intensita_collaborativa_cum"
        )
        
        # 4. Esegui l'analisi
        self._execute_process_analysis_loop(sampled_repos)

    def _run_giganti_no_collab_popolari(self):
        print("\n--- [Q1] Avvio Analisi: Collaborazione 'Sana' vs 'Inefficace' ---")
        
        # 1. Carica il file stratificato (l'unica dipendenza)
        try:
            stratified_df = self.provider.get_stratified_data()
        except Exception as e:
            print(f"❌ ERRORE: Impossibile caricare il file stratificato. Dettagli: {e}")
            return

        # 2. Definizione archetipi (modificata)
        profiles_to_analyze = {
            "Giganti Non Collaborativi Popolari": (
                (pl.col("popolarita_esterna_norm_cat").is_in(["Gigante"])) &
                (pl.col("intensita_collaborativa_norm_cat").is_in(["Zero", "Basso"])) &
                (pl.col("volume_lavoro_norm_cat").is_in(["Zero", "Basso"])) &
                (pl.col("engagement_community_norm_cat").is_in(["Zero", "Basso"]))
            )
        }
        
        # 3. Seleziona i campioni usando i profili definiti
        sampled_repos = domain_services.select_representative_repos(
            stratified_df=stratified_df,
            config=self.config,
            archetype_profiles=profiles_to_analyze,
            sampling_metric="intensita_collaborativa_cum"
        )
        
        # 4. Esegui l'analisi
        self._execute_process_analysis_loop(sampled_repos)


    """def _run_q1_analysis(self):
        DOMANDA 1: Esiste un processo 'tossico' vs 'sano'?
        print("\n--- [Q1] Avvio Analisi: Collaborazione 'Sana' vs 'Inefficace' ---")
        
        # 1. Carica il file stratificato (l'unica dipendenza)
        try:
            stratified_df = self.provider.get_stratified_data()
        except Exception as e:
            print(f"❌ ERRORE: Impossibile caricare il file stratificato. Dettagli: {e}")
            return

        # 2. Definizione archetipi
        profiles_to_analyze = {
            "Stella Nascente Collaborativa": (
                (pl.col("intensita_collaborativa_norm_cat").is_in(["Gigante", "Alto"])) &
                (pl.col("volume_lavoro_norm_cat").is_in(["Gigante", "Alto"]))
            ),
            "Collaborazione Inefficace": (
                (pl.col("intensita_collaborativa_norm_cat").is_in(["Alto", "Gigante"])) &
                (pl.col("popolarita_esterna_cum_cat").is_in(["Zero", "Basso"])) &
                (pl.col("volume_lavoro_norm_cat").is_in(["Zero", "Basso"]))
            )
        }
        
        # 3. Seleziona i campioni usando i profili definiti
        sampled_repos = domain_services.select_representative_repos(
            stratified_df=stratified_df,
            config=self.config,
            archetype_profiles=profiles_to_analyze,
            n_per_archetipo=self.config.n_per_strato
        )
        
        # 4. Esegui l'analisi (il loop rimane invariato)
        self._execute_process_analysis_loop(sampled_repos)

    def _run_q2_analysis(self):
        DOMANDA 2: Il processo cambia dopo l'hype?
        print("\n--- [Q2] Avvio Analisi Temporale: Impatto dell'Hype sui Processi ---")
        
        # 1. Carica il file stratificato
        try:
            stratified_df = self.provider.get_stratified_data()
        except (FileNotFoundError, IOError) as e:
            print(f"❌ ERRORE DI PRECONDIZIONE: {e}")
            return
        
        # 2. Definisci il profilo dell'archetipo target
        profiles_to_analyze = {
            "Progetto Vetrina (Hype Puro)": (
                (pl.col("popolarita_esterna_norm_cat").is_in(["Gigante", "Alto"])) &
                (pl.col("volume_lavoro_cum_cat").is_in(["Zero", "Basso"])) &
                (pl.col("intensita_collaborativa_cum_cat").is_in(["Zero"]))
            )
        }
        
        # 3. Seleziona i campioni
        sampled_repos = domain_services.select_representative_repos(
            stratified_df=stratified_df,
            config=self.config,
            archetype_profiles=profiles_to_analyze,
            n_per_archetipo=self.config.n_per_strato
        )

        # 4. Esegui il ciclo di analisi temporale specifico per questa domanda
        archetype_target = "Progetto Vetrina (Hype Puro)"
        for repo_id in sampled_repos.get(archetype_target, []):
            print(f"\n--- Analisi Temporale di Repo {repo_id} ---")
            
            try:
                split_logs = self.provider.get_events_for_repo_split_by_peak(repo_id, "WatchEvent")
            except Exception as e:
                print(f"⚠️ Errore durante la divisione del log per {repo_id}: {e}")
                continue # Passa alla repo successiva
                
            if not split_logs:
                print(f"⚠️ Impossibile dividere il log per {repo_id} (es. nessun WatchEvent), passo al successivo.")
                continue
                
            log_prima, log_dopo = split_logs
            
            print(f"Analisi fase 'prima' del picco...")
            results_before = self.analyzer.analyze_repo_process(log_prima, repo_id)
            if results_before:
                self.writer.save_analysis_artifacts(results_before, repo_id, archetype_target, suffix="prima")

            print(f"Analisi fase 'dopo' il picco...")
            results_after = self.analyzer.analyze_repo_process(log_dopo, repo_id)
            if results_after:
                self.writer.save_analysis_artifacts(results_after, repo_id, archetype_target, suffix="dopo")

    # =============================================================
    # VERSIONE CORRETTA E AGGIORNATA DI _run_q3_analysis
    # =============================================================
    def _run_q3_analysis(self):
        DOMANDA 3: Come nasce una Code Review?
        print("\n--- [Q3] Avvio Analisi Evolutiva: Nascita della Code Review ---")
        
        # 1. Carica il file stratificato
        try:
            stratified_df = self.provider.get_stratified_data()
        except (FileNotFoundError, IOError) as e:
            print(f"❌ ERRORE DI PRECONDIZIONE: {e}")
            return
            
        # 2. Definisci il profilo dell'archetipo target
        profiles_to_analyze = {
            "Prima Scintilla Collaborativa": (
                (pl.col("volume_lavoro_cum_cat").is_in(["Medio", "Alto"])) &
                (pl.col("intensita_collaborativa_cum_cat") == "Basso") &
                (pl.col("engagement_community_cum_cat") == "Zero")
            )
        }
        
        num_samples = 15  # Vogliamo un campione più grande per questa analisi
        print(f"Selezione di {num_samples} campioni...")

        # 3. Seleziona i campioni
        sampled_repos = domain_services.select_representative_repos(
            stratified_df=stratified_df,
            config=self.config,
            archetype_profiles=profiles_to_analyze,
            n_per_archetipo=num_samples
        )
        
        # 4. Esegui il loop di analisi standard
        self._execute_process_analysis_loop(sampled_repos)"""

    # --- METODO HELPER PER L'ESECUZIONE DELL'ANALISI DI PROCESSO ---
    def _execute_process_analysis_loop(self, sampled_repos: Dict[str, List[str]]):
        """
        Cicla su un dizionario di repo campionate ed esegue l'analisi di processo per ciascuna.
        """
        repo_to_archetype = {
            repo_id: archetype
            for archetype, ids in sampled_repos.items()
            for repo_id in ids
        }

        if not repo_to_archetype:
            print("⚠️ Nessuna repository campionata per l'analisi. Termino.")
            return

        print(f"\nInizio analisi di processo su {len(repo_to_archetype)} repository campionate...")
        for repo_id, archetype_name in repo_to_archetype.items():
            print(f"\n--- Analizzando Repo {repo_id} (Archetipo: {archetype_name}) ---")

            # 1. Carica i dati
            repo_lazy_frame = self.provider.get_lazy_events_for_repo(repo_id) 
            
            # 2. Esegui l'analisi
            analysis_results = self.analyzer.analyze_repo_process(repo_lazy_frame, repo_id)
            
            # 3. Salva i risultati
            if analysis_results:
                self.writer.save_analysis_artifacts(
                    results=analysis_results,
                    repo_id=repo_id,
                    archetype_name=archetype_name,
                    suffix="str"
                )
            else:
                print(f"[Pipeline] ⚠️ Nessun risultato di analisi generato per {repo_id}.")

    def _run_summary_analysis(self):
        """
        Scansiona TUTTI i risultati delle analisi di processo (.pkl), estrae un set completo
        di KPI quantitativi utilizzando il ModelAnalyzer e genera una singola tabella di 
        sintesi CSV per l'analisi comparativa finale.
        """
        print("\n--- [SUMMARIZE] Avvio sintesi quantitativa completa dei risultati ---")
        
        results_base_dir = os.path.join(self.config.analysis_base_dir, "process_analysis")
        if not os.path.isdir(results_base_dir):
            print(f"⚠️ La directory dei risultati '{results_base_dir}' non è stata trovata. "
                  "Esegui prima le pipeline di analisi degli archetipi per generare i modelli.")
            return

        all_kpis = []
        
        print(f"[Pipeline] Scansione di '{results_base_dir}' alla ricerca di modelli .pkl...")
        # La logica di scansione è agnostica e funziona per qualsiasi archetipo analizzato
        for archetype_name in os.listdir(results_base_dir):
            archetype_dir = os.path.join(results_base_dir, archetype_name)
            if not os.path.isdir(archetype_dir): continue
            
            for repo_id in os.listdir(archetype_dir):
                repo_dir = os.path.join(archetype_dir, repo_id)
                if not os.path.isdir(repo_dir): continue
                
                print(f"  -> Analizzando i modelli per {repo_id} ({archetype_name})")
                
                models_by_phase = {}
                for filename in os.listdir(repo_dir):
                    if not filename.endswith(".pkl"): continue
                    phase = "str" # Assumiamo analisi non temporali, come da nuovo focus
                    if "_prima" in filename: phase = "prima"
                    if "_dopo" in filename: phase = "dopo"
                    
                    models_by_phase.setdefault(phase, {})
                    if 'frequency_model' in filename:
                        models_by_phase[phase]['freq_path'] = os.path.join(repo_dir, filename)
                    elif 'performance_model' in filename:
                        models_by_phase[phase]['perf_path'] = os.path.join(repo_dir, filename)

                if not models_by_phase:
                    print(f"     ⚠️ Nessun file .pkl valido trovato per {repo_id}.")
                    continue

                for phase, model_paths in models_by_phase.items():
                    freq_path = model_paths.get('freq_path')
                    perf_path = model_paths.get('perf_path')

                    if not freq_path or not perf_path:
                        print(f"     ⚠️ Fase '{phase}' per {repo_id} incompleta. Skippata.")
                        continue

                    print(f"     - Processando fase: '{phase}'")

                    freq_model = self.model_analyzer.load_model_from_file(freq_path)
                    perf_model = self.model_analyzer.load_model_from_file(perf_path)
                    
                    if not freq_model or not perf_model:
                        print(f"     ⚠️ Modelli .pkl per la fase '{phase}' di {repo_id} illeggibili. Skippata.")
                        continue
                        
                    # =======================================================
                    # --- ESTRAZIONE DEL SET DI KPI COMPLETO E POTENZIATO ---
                    # =======================================================
                    kpis = {
                        "repo_id": repo_id,
                        "archetype": archetype_name,
                        "phase": phase if phase != "str" else None,
                        
                        # KPI di Complessità
                        "num_activities": self.model_analyzer.count_activities(freq_model),
                        "num_connections": self.model_analyzer.count_connections(freq_model),
                        
                        # KPI di Processo Collaborativo Interno
                        "has_review_loop": self.model_analyzer.has_review_loop(freq_model),
                        "review_cycle_time_hours": self.model_analyzer.get_review_cycle_performance(perf_model),
                        "pr_lead_time_hours": self.model_analyzer.get_pr_lead_time(perf_model),

                        # KPI di Interazione Esterna (Community)
                        "is_issue_driven": self.model_analyzer.is_issue_driven(freq_model),
                        "issue_reaction_time_hours": self.model_analyzer.get_issue_reaction_time(perf_model),
                    }
                    all_kpis.append(kpis)

        if not all_kpis:
            print("⚠️ Nessun KPI è stato estratto. Controlla che le pipeline di analisi siano state eseguite correttamente.")
            return

        summary_df = pl.from_dicts(all_kpis)
        self.writer.save_quantitative_summary(summary_df, "quantitative_summary.csv")

    """def run(self):
        Metodo principale che esegue l'intera pipeline in sequenza.
        print("[Pipeline] Avvio pipeline completa...")
        
        # --- Fase 1: Feature Engineering (Calcolo Metriche per Stratificazione) ---
        print("\n--- [FASE 1/3] Feature Engineering ---")
        eligible_ids = self.provider.get_eligible_repo_ids()
        ids_path = self.config.eligible_ids_path
        self.writer.save_id_list(list(eligible_ids), Path(ids_path).name)

        # Rilascio memoria usata da eligible_ids
        del eligible_ids
        gc.collect()
        print("[1A/4] Checkpoint degli ID salvato su disco. Memoria rilasciata.")
        
        # STRANDARD A CHUNK
        metrics_entities: List[RepositoryMetrics] = self.provider.calculate_summary_metrics_from_file(ids_path)
        self.writer.save_entities_as_dataframe(metrics_entities, Path(self.config.metrics_file_path).name)
    
        # STANDARD
        metrics_list = self.provider.calculate_summary_metrics(eligible_ids)
        self.writer.save_lazyframe(metrics_list, Path(self.config.metrics_file_path).name)

        # GOLD
        #metrics_df_gold = self.provider.calculate_summary_metrics_gold(eligible_ids)
        #self.writer.save_lazyframe(metrics_df_gold, Path(self.config.metrics_gold_file_path).name)

        # STANDARD CON AGE
        metrics_list_age = self.provider.calculate_summary_metrics(eligible_ids)
        self.writer.save_lazyframe(metrics_list_age, Path(self.config.metrics_age_file_path).name)
        
        # --- Fase 2: Stratificazione e Campionamento ---
        print("\n--- [FASE 2/3] Stratificazione e Campionamento ---")
        stratified_df, soglie = domain_services.stratify(self.config.metrics_age_file_path, config=self.config) # modifica input str/age/gold
        self.writer.save_dataframe(stratified_df, Path(self.config.stratified_file_path).name)
        self.writer.save_json(soglie, Path(self.config.thresholds_file_path).name)
         
        # Report sulla distribuzione
        counts_df = domain_services.count_by_stratum(stratified_df)
        print("Distribuzione repository per strato:")
        print(counts_df)
        
        sample_df = domain_services.sample(stratified_df, config=self.config)
        self.writer.save_dataframe(sample_df, Path(self.config.sample_info_path).name)
        self.writer.save_id_list(sample_df["repo_id"].to_list(), Path(self.config.sample_ids_path).name)
        print(f"Creato campione di {len(sample_df)} repo.")

        # --- Fase 3: Dstribuzione degli strati ---
        # Percorso di output per la distribuzione degli strati
        strata_count_path = Path(self.config.analysis_base_dir) / "strata_distribution.csv"

        # Calcola i conteggi per ogni strato
        counts_df = domain_services.count_by_stratum(stratified_df)

        # Aggiunge una colonna percentuale cumulativa (opzionale ma utile)
        counts_df = counts_df.with_columns(
            (pl.col("num_repo") / pl.col("num_repo").sum() * 100).round(3).alias("perc_tot")
        )

        # Salva il risultato come CSV
        counts_df.write_csv(strata_count_path)

        print(f"[Writer] ✅ Distribuzione per strato salvata in: {strata_count_path}")

        summary_df = domain_services.summarize_archetypes(
            strata_distribution_path=os.path.join(self.config.analysis_base_dir, "strata_distribution.csv"),
            config=self.config,
            output_path=os.path.join(self.config.analysis_base_dir, "archetype_summary.csv")
        )

        # --- Fase 4: Extrazione repo ---
        sampled_repos = domain_services.select_representative_repos(
            stratified_path=self.config.stratified_file_path,
            config=self.config,
            n_per_archetipo=3,
            seed=self.config.RANDOM_SEED
        )

        print("\n--- [FASE 5/5] Analisi dei Processi (Heuristic Miner) ---")
        all_repo_ids = [repo_id for ids in sampled_repos.values() for repo_id in ids]
        # Iteriamo sul campione e deleghiamo l'analisi all'analyzer
        for repo_id in all_repo_ids:
            repo_lazy_frame = self.provider.get_lazy_events_for_repo(repo_id) 
            analysis_results = self.analyzer.analyze_repo_new(repo_lazy_frame, repo_id)
            
            if analysis_results:
                heu_net_freq, heu_net_perf, log = analysis_results
                
                output_path = os.path.join(self.config.analysis_base_dir, f"{repo_id}_{"workflow_model_freq"}.png")
                pm4py.save_vis_heuristics_net(heu_net_freq, output_path)
                
                output_path = os.path.join(self.config.analysis_base_dir, f"{repo_id}_{"workflow_model_perf"}.png")
                pm4py.save_vis_heuristics_net(heu_net_perf, output_path)
                
                output_path = os.path.join(self.config.analysis_base_dir, f"{repo_id}_log.xes")
                xes_exporter.apply(log, output_path)


        # --- Fase 3: Caricamento Dati del Campione ---
        #print("\n--- [FASE 3/4] Caricamento Dati del Campione ---")
        #sample_events_df = self.provider.get_events_for_repos(sample_df["repo_id"].to_list())


        # --- Fase 3: Process Mining sul Campione ---
        print(f"\n--- [FASE 3/3] Process Mining su {len(sample_df)} Repository ---")
        all_kpis: List[Dict[str, Any]] = []
        
        # Iteriamo sul campione e deleghiamo l'analisi all'analyzer
        for row in sample_df.iter_rows(named=True):
            repo_id = str(row["repo_id"])
            strato_id = int(row["strato_id"])

            print(f"\n--- Analizzando Repo {repo_id} (Strato {strato_id}) ---")
            
            repo_lazy_frame = self.provider.get_lazy_events_for_repo(repo_id)
            
            analysis_results = self.analyzer.analyze_repo(repo_lazy_frame, repo_id, strato_id)
            
            if analysis_results:
                kpis, heu_net_freq, heu_net_perf, log = analysis_results
                all_kpis.append(kpis)
                
                # Deleghiamo il salvataggio degli artefatti al writer
                self.writer.save_heuristics_net(heu_net_freq, repo_id, strato_id, suffix="workflow_model_freq")
                self.writer.save_heuristics_net(heu_net_perf, repo_id, strato_id, suffix="workflow_model_perf")
                self.writer.save_log_xes(log, repo_id, strato_id)
        
        # --- Salvataggio Finale dei KPI ---
        print("\n[Pipeline] Salvataggio tabella KPI consolidata...")
        self.writer.save_kpis(all_kpis)
        
        print("\n[OK] Pipeline completata con successo.")"""