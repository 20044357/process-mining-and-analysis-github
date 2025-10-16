import argparse
import os
import shutil

from .manager import DatasetManager
from .utils import parse_hour, folder_size_mb, infer_date_range


def _get_dataset_path(args) -> str:
    """
    Restituisce il percorso della cartella base del dataset distillato.

    Ordine di priorità:
        1. Argomento CLI `--config-path`
        2. Variabile d'ambiente `DATASET_PATH`
        3. Default: "data/dataset_distillato"

    Args:
        args: Namespace degli argomenti CLI.

    Returns:
        str: Percorso della cartella base del dataset.
    """
    return args.config_path or os.environ.get("DATASET_PATH", "data/dataset_distillato")


def run_download(args) -> None:
    """
    Avvia un job di download e distillazione per un range orario.

    - Converte le stringhe di input in datetime.
    - Verifica che l'ora di fine sia successiva a quella di inizio.
    - Richiama DatasetManager per scaricare e distillare l'intervallo.
    - Stampa un riepilogo dei conteggi.

    Args:
        args: Namespace con attributo `download` = [start, end].
    """    
    base_dir = _get_dataset_path(args)
    manager = DatasetManager(base_output_dir=base_dir)

    try:
        start_time = parse_hour(args.download[0])
        end_time = parse_hour(args.download[1])

        if end_time < start_time:
            raise SystemExit("[ERROR]: END precedente a START.")

        print(f"[START] Download degli eventi tra {start_time} e {end_time}...")

        total, distilled, discarded = manager.process_range(start_time, end_time)
        print(f"[SUMMARY] total={total} distilled={distilled} bad={discarded}")
    except ValueError as e:
        print(f"[ERROR]: input di data non valido. Dettagli: {e}")


def show_dataset_info(args) -> None:
    """
    Mostra informazioni sul dataset locale:
        - Percorso assoluto
        - Dimensione su disco
        - Range orario coperto (se disponibile)

    Args:
        args: Namespace CLI con eventuale `config_path`.
    """    
    base_dir = _get_dataset_path(args)
    print(f"Percorso dataset: {os.path.abspath(base_dir)}")
    if not os.path.exists(base_dir):
        print("Il dataset locale non esiste.")
        return

    print(f"Dimensione attuale: {folder_size_mb(base_dir):.2f} MB")
    date_range = infer_date_range(base_dir)
    if date_range:
        print(f"Ore presenti: {date_range[0]} → {date_range[1]}")
    else:
        print("Nessuna ora deducibile (manca index.json o hours_processed).")


def reset_dataset(args) -> None:
    """
    Cancella completamente il dataset locale (cartella base).

    Args:
        args: Namespace CLI con eventuale `config_path`.
    """    
    base_dir = _get_dataset_path(args)
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
        print(f"Dataset resettato: {base_dir}")
    else:
        print("Nessun dataset da resettare.")


def main():
    """
    Entry point principale del programma CLI.
    Gestisce il parsing degli argomenti e indirizza ai comandi richiesti:
        --download → run_download
        --info     → show_dataset_info
        --reset    → reset_dataset
    """
    parser = argparse.ArgumentParser(
        prog="dataset_ingestor",
        description="GestoreDataset — download/distillazione GHArchive e gestione dataset locale.",
    )

    parser.add_argument(
        "--config-path",
        help="Cartella del Dataset Distillato (default: env DATASET_PATH o ./dataset_distillato).",
        default=None,
    )

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "--download",
        nargs=2,
        metavar=("START", "END"),
        help="Scarica e distilla il range orario [YYYY-MM-DD-HH YYYY-MM-DD-HH].",
    )

    group.add_argument(
        "--info",
        action="store_true",
        help="Mostra informazioni sul dataset locale (path, range, dimensione).",
    )

    group.add_argument(
        "--reset",
        action="store_true",
        help="Cancella completamente il dataset locale.",
    )

    args = parser.parse_args()

    if args.reset:
        return reset_dataset(args)
    if args.info:
        return show_dataset_info(args)
    if args.download:
        return run_download(args)


if __name__ == "__main__":
    main()
