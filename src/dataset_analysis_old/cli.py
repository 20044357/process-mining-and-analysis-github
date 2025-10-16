import argparse
import os
import sys

import pandas as pd

from .core import extract_create_repo_events
from .metrics.metrics_from_dataset import calculate_success_score_only, calculate_activity_summary
from .metrics.metrics_from_api import calculate_api_metrics
from .mining import run_mining
from .writers import MetricsWriter
from .stratification import run_stratification
import polars as pl

from dotenv import load_dotenv
load_dotenv()

def _get_dataset_path(args) -> str:
    """
    Restituisce il percorso della cartella base del dataset distillato.

    Ordine di priorità:
        1. Argomento CLI `--config-path`
        2. Variabile d'ambiente `DATASET_PATH`
        3. Default: "data/dataset_distillato"
    """
    return args.config_path or os.environ.get("DATASET_PATH", "data/dataset_distillato")


def _get_analysis_path() -> str:
    """
    Restituisce il percorso della cartella base dei dati di analisi.

    Ordine di priorità:
        1. Variabile d'ambiente `DATA_ANALYSIS`
        2. Default: "data/dataset_analyzed"
    """
    return os.environ.get("DATA_ANALYSIS", "data/dataset_analyzed")


def run_analyze(args) -> None:
    """
    Orchestra l'intera pipeline di analisi.

    Args:
        args: Namespace degli argomenti CLI.
    """

    
    """
    # 1. Calcolo solo success_score
    success_tbl = calculate_success_score_only(base_dir_input, repo_ids)
    df_success = success_tbl.to_pandas()
    df_success = df_success.sort_values(
        by=["success_score", "repo_id"],
        ascending=[False, True]   # prima ordina per punteggio, poi per ID
    ).reset_index(drop=True)

    
    # 2. Top 1200 (= 1200 * 4 (API) = 4800 richieste API (limit rate: 5000 richiesta max in 1 hour))
    df_success = df_success.head(1200) # MOMENTANEO
    top_repo_ids = set(df_success["repo_id"].tolist()) # MOMENTANEO
    print(f"Repo selezionate per analisi completa: {len(top_repo_ids)}")
    
    #print(f"Repo prime 11: {df_success["repo_id"].head(11)}\n")
    #print(f"Repo centrali 11: {df_success["repo_id"].iloc[2014262:2014272]}\n")
    #print(f"Repo ultime 11: {df_success["repo_id"].tail(11)}\n")    

    #filtered = pd.concat([df_success.head(11), df_success.loc[2014262:2014272], df_success.tail(11)])
    filtered = df_success.iloc[[
        1, 2, 3, 4, 5,
        100, 101, 102, 103, 104, 105,
        1000, 1001, 1002, 1003, 1004, 1005,
        100000, 100001, 100002, 100003, 100004, 100005, 
        1000000, 1000001, 1000002, 1000003, 1000004, 1000005
        ]]


    filtered = df_success.iloc[[
        1,
        10,
        100,
        1000
        ]]

    print(f"Attualmente analizzo solo le repo:\n {filtered.to_string(index=False)}\n")
    #df_success = df_success.head(10)
    #df_success = df_success.queue(10)
    top_repo_ids = set(filtered["repo_id"].tolist())
    
    """
    base_dir_input = _get_dataset_path(args)
    base_dir_output = _get_analysis_path()

    print(f"[START] Avvio pipeline di analisi su {base_dir_input}...")
    
    
    # 1. Estrazione repo nate nel range temporale
    repo_ids = extract_create_repo_events(base_dir_input)
    print(f"Numero di repo nate nel range dataset: {len(repo_ids)}")
    if len(repo_ids) == 0:
        print("[WARNING] Nessuna repo nata nel range temporale del dataset.")
        return


    # 2. Calcolo metriche statiche
    #print("[...] Calcolo metriche statiche...")
    #static_tbl = calculate_static_metrics(base_dir_input, top_repo_ids)
    #df_static_extra = static_tbl.to_pandas()

    # 2. Calcolo delle metriche di attività e salvataggio nel file CSV.
    df_metrics = calculate_activity_summary(base_dir_input, repo_ids)
    df_metrics = df_metrics.sort("num_watch", descending=True)
    writer = MetricsWriter(base_dir_output)
    writer.save_polars_csv(df_metrics, filename="metrics.csv")

    # 3. Stratificazione e Campionamento
    print("[...] Stratificazione e campionamento...")
    sample = run_stratification(
        metrics_path=os.path.join(base_dir_output, "metrics.csv")
    )
    print(f"[OK] Campione creato: {len(sample)} repo selezionate")

    # 4. Process Mining e Calcolo KPI
    print("[...] Process Mining e KPI...")
    results_all_repo = run_mining(base_dir_input, sample)

    """
    # 3. Calcolo metriche API (4800 richieste API)
    print("[...] Calcolo metriche API...")
    df_api = calculate_api_metrics(base_dir_input, top_repo_ids)

    # 4. Analisi comportamentale (nuovo step) (top_repo_ids)
    print("[...] Analisi comportamentale (Social, Attenzione, Process Mining)...")
    beh = run_mining(base_dir_input, top_repo_ids)"""

def run_behavior_only(args) -> None:
    base_dir_input = _get_dataset_path(args)

    # 4. Analisi comportamentale (nuovo step) (top_repo_ids)
    #repo_id = {1052509990}
    print("[...] Analisi comportamentale (Social, Attenzione, Process Mining)...")
    #beh = run_mining(base_dir_input, repo_id)

    print("[OK] Analisi comportamentale completata.")

def main():
    """
    Entry point principale del programma CLI.
    """
    parser = argparse.ArgumentParser(
        prog="dataset_ingestor",
        description="GestoreDataset — esecuzione pipeline di analisi sul Dataset Distillato.",
    )

    parser.add_argument(
        "--config-path",
        help="Cartella del Dataset Distillato (default: env DATASET_PATH o ./data/dataset_distillato).",
        default=None,
    )
    
    # Comando permesso: --analyze
    parser.add_argument(
        "--analyze",
        action="store_true",
        help="Esegue la pipeline di analisi su tutte le repository del dataset distilato."
    )

    parser.add_argument(
        "--behavior",
        action="store_true",
        help="Esegue solo la fase di analisi comportamentale sulle metriche già salvate."
    )

    args = parser.parse_args()
    
    if args.analyze:
        run_analyze(args)
    elif args.behavior:
        run_behavior_only(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()