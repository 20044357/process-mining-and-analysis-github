# File: tests/e2e/test_cli.py
import subprocess
import os
from pathlib import Path
import sys

def test_cli_full_pipeline_e2e(test_parquet_dataset, tmp_path):
    """
    Testa l'esecuzione completa della pipeline 'full' tramite la CLI,
    passando i percorsi come argomenti diretti.
    """
    # 1. Setup
    dataset_dir = test_parquet_dataset
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    project_root = Path(__file__).resolve().parents[2]

    # Le variabili d'ambiente per le date sono ancora necessarie
    test_env = os.environ.copy()
    test_env["ANALYSIS_START_DATE"] = "2023-01-01T00:00:00Z"
    test_env["ANALYSIS_END_DATE"] = "2023-01-03T23:59:59Z"

    module_path = "src.dataset_analysis.infrastructure.cli"

    # --- INIZIO CORREZIONE CHIAVE ---
    # Costruisci il comando passando i percorsi come argomenti CLI.
    # Questo bypassa la necessità di variabili d'ambiente per i percorsi.
    command = [
        sys.executable, "-m", module_path, "full",
        "--dataset-dir", str(dataset_dir),
        "--analysis-dir", str(output_dir)
    ]
    # --- FINE CORREZIONE CHIAVE ---

    # 2. Esecuzione del comando -m (modalità modulo)
    result = subprocess.run(
        command, # Usa la lista di comandi costruita sopra
        capture_output=True,
        text=True,
        env=test_env, # Passiamo solo le variabili per le date
        check=False,
        cwd=project_root
    )

    print("\n--- INIZIO OUTPUT SUBPROCESS ---")
    print(f"Return Code: {result.returncode}")
    print("\n[STDOUT]:")
    print(result.stdout)
    print("\n[STDERR]:")
    print(result.stderr)
    print("--- FINE OUTPUT SUBPROCESS ---\n")

    # 3. Debug e Assert
    if result.returncode != 0:
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)

    # Controlla che lo script sia terminato con successo
    assert result.returncode == 0, "Lo script CLI si è concluso con un errore."

    # Controlla che i file di output siano stati creati
    assert (output_dir / "metrics.parquet").exists(), "Il file 'metrics.parquet' non è stato creato."
    assert (output_dir / "repo_metrics_stratified.parquet").exists(), "Il file stratificato non è stato creato."
    assert (output_dir / "strata_distribution.csv").exists(), "Il file di distribuzione non è stato creato."