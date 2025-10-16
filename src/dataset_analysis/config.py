from dataclasses import dataclass
import os

@dataclass
class AnalysisConfig:
    """Centralizza tutti i parametri e i percorsi del progetto."""
    # Percorsi di base (configurabili da CLI o env vars)
    dataset_base_dir: str
    analysis_base_dir: str

    # Parametri della finestra temporale
    analysis_start_date: str
    analysis_end_date: str
    
    # Parametri di stratificazione
    n_per_strato: int = 2
    sampling_seed: int = 42
    
    # --- Parametri di Campionamento ---
    QUANTILES = [0.5, 0.9, 0.99]
    QUANTILE_LABELS = ["Basso", "Medio", "Alto", "Gigante"]
    MIN_SAMPLES_PER_STRATUM = 10
    PROPORTIONAL_SAMPLE_RATE = 0.0001 # 0.01%
    RANDOM_SEED = 42

    # Parametri di mining
    #hm_dependency_threshold: float = 0.2 # Soglia "Esplorativa"

    # Parametro per il chunking
    chunk_size: int = 30000
    
    # Nomi dei file di output
    @property
    def eligible_ids_path(self) -> str:
        return os.path.join(self.analysis_base_dir, "eligible_repo_ids.txt")
        
    @property
    def metrics_age_file_path(self) -> str:
        return os.path.join(self.analysis_base_dir, "metrics_age.parquet")

    @property
    def metrics_gold_file_path(self) -> str:
        return os.path.join(self.analysis_base_dir, "metrics_gold.parquet")

    @property
    def metrics_file_path(self) -> str:
        return os.path.join(self.analysis_base_dir, "metrics.parquet")

    @property
    def thresholds_file_path(self) -> str:
        return os.path.join(self.analysis_base_dir, "stratification_thresholds.json")
    
    @property
    def stratified_file_path_parquet(self) -> str:
        return os.path.join(self.analysis_base_dir, "repo_metrics_stratified.parquet")

    @property
    def stratified_file_path_csv(self) -> str:
        return os.path.join(self.analysis_base_dir, "repo_metrics_stratified.csv")

    @property
    def strata_distribution_file_path_csv(self) -> str:
        return os.path.join(self.analysis_base_dir, "strata_distribution.csv")

    @property
    def sample_info_path(self) -> str:
        return os.path.join(self.analysis_base_dir, "campione_stratificato.csv")

    @property
    def sample_ids_path(self) -> str:
        return os.path.join(self.analysis_base_dir, "campione_stratificato_ids.txt")

    @property
    def kpi_results_path(self) -> str:
        return os.path.join(self.analysis_base_dir, "kpi_results.csv")