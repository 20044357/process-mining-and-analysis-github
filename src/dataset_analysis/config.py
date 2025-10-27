from dataclasses import dataclass
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

    # ======================================================
    #                 Percorsi dei file di output
    # ======================================================

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