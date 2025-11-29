from dataclasses import dataclass, field
import os

@dataclass
class AnalysisConfig:
    """
    Questa classe definisce tutti i parametri di configurazione globali e i percorsi
    dei file generati dal sistema. È condivisa tra i vari layer dell'architettura
    (Application, Domain, Infrastructure) per garantire coerenza e tracciabilità.
    """

    # --- Directory principali ---
    dataset_directory: str        # Directory contenente i dataset sorgente (file Parquet di GitHub)
    output_directory: str         # Directory di destinazione per i risultati dell'analisi

    # --- Finestra temporale di analisi ---
    start_date: str               # Data di inizio del periodo analizzato
    end_date: str                 # Data di fine del periodo analizzato

    # --- Costanti statistiche per la suddivisione dei gruppi ---
    QUANTILES = [0.5, 0.9, 0.99]
    QUANTILE_LABELS = ["Low", "Medium", "High", "Giant"]

    # --- Costanti per i nomi delle directory di output ---
    archetype_models_subdirectory_name: str = "archetype_models"


    @property
    def analyzable_repositories_file(self) -> str:
        """repo_id delle repository nate nel range del dataset."""
        return os.path.join(self.output_directory, "analyzable_repositories.csv")

    @property
    def raw_metrics_file(self) -> str:
        """Metriche grezze calcolate direttamente sugli eventi originari."""
        return os.path.join(self.output_directory, "metrics_raw.parquet")

    @property
    def stratification_thresholds_file(self) -> str:
        """Soglie numeriche calcolate per la definizione dei gruppi di stratificazione."""
        return os.path.join(self.output_directory, "stratification_thresholds.json")

    @property
    def stratified_repositories_parquet(self) -> str:
        """Risultati stratificati (formato Parquet) con l'appartenenza di ciascun repository al relativo gruppo."""
        return os.path.join(self.output_directory, "repositories_stratified.parquet")

    @property
    def stratified_repositories_csv(self) -> str:
        """Risultati stratificati (formato CSV) utilizzabili per analisi esplorative."""
        return os.path.join(self.output_directory, "repositories_stratified.csv")

    @property
    def group_distribution_file(self) -> str:
        """Distribuzione complessiva delle repository nei vari gruppi di stratificazione."""
        return os.path.join(self.output_directory, "group_distribution.csv")

    @property
    def quantitative_summary_file(self) -> str:
        """Tabella di sintesi finale con tutti i KPI calcolati."""
        return os.path.join(self.output_directory, "quantitative_summary.csv")
    
    @property
    def individual_kpis_report_file(self) -> str:
        """Report CSV con i KPI calcolati per ogni singola repository campionata."""
        return os.path.join(self.output_directory, "individual_kpis_report.csv")

    @property
    def aggregate_kpis_report_file(self) -> str:
        """Report CSV con i KPI aggregati calcolati per ogni archetipo."""
        return os.path.join(self.output_directory, "aggregate_comparison_report.csv")

    @property
    def final_analysis_directory(self) -> str:
        """Directory radice per i risultati dell'analisi avanzata."""
        return os.path.join(self.output_directory, "final_analysis")

    @property
    def archetype_models_directory(self) -> str:
        """Directory per i modelli di processo archetipici."""
        return os.path.join(self.final_analysis_directory, "archetype_models")
    
    @property
    def diagnostics_directory(self) -> str:
        """Directory per i report di diagnostica."""
        return os.path.join(self.final_analysis_directory, "diagnostics")

    @property
    def recommendations_directory(self) -> str:
        """Directory per i report di raccomandazione."""
        return os.path.join(self.final_analysis_directory, "recommendations")

    @property
    def health_comparison_report_file(self) -> str:
        """File CSV con il confronto dei KPI di salute."""
        return os.path.join(self.final_analysis_directory, "health_comparison_report.csv")
    
    @property
    def process_analysis_directory(self) -> str:
        """Directory radice per tutti gli artefatti di analisi di processo."""
        return os.path.join(self.output_directory, "process_analysis")
    
    @property
    def structural_comparison_directory(self) -> str:
        """Directory per i modelli di processo archetipici."""
        return os.path.join(self.final_analysis_directory, "structural_comparison")
    
    @property
    def archetype_process_models_directory(self) -> str:
        """Directory per salvare i modelli di processo per ogni archetipo."""
        return os.path.join(
            self.process_analysis_directory,
            self.archetype_models_subdirectory_name
        )