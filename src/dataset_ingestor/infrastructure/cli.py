import argparse
import logging
import os
import shutil
import sys
from datetime import timedelta
from dotenv import load_dotenv

from .utils import parse_hour, folder_size_mb, get_dataset_summary
from ..application.interfaces import IIngestionUseCase
from ..application.use_cases import IngestionService
from ..application.errors import IngestionError
from .gharchive_source import GhArchiveEventSource
from .json_index_repository import JsonIngestionIndexRepository
from .logging_config import configure_logging, LayerLoggerAdapter

def _get_dataset_path(args) -> str:
    """Restituisce il percorso della cartella base del dataset."""
    return args.config_path or os.environ.get("DATASET_PATH", "data/dataset_distillato")

def run_download(args, use_case: IIngestionUseCase, logger: LayerLoggerAdapter) -> None:
    """Avvia un job di download per un intervallo di almeno due ore."""
    start_time = parse_hour(args.download[0])
    end_time = parse_hour(args.download[1])

    if not start_time or not end_time:
        logger.warning("Intervallo orario non valido o malformato. Nessuna operazione eseguita.")
        return

    if end_time < start_time + timedelta(hours=1):
        logger.warning("Intervallo troppo corto (< 1h). Nessuna operazione eseguita.")
        return

    logger.info(f"Avvio download del range di eventi da {start_time} a {end_time}...")
    total, distilled, discarded = use_case.process_time_range(start_time, end_time)
    logger.info(f"Riepilogo del range: parsed={total}, distilled={distilled}, bad={discarded}")

def run_hours(args, use_case: IIngestionUseCase, logger: LayerLoggerAdapter) -> None:
    """
    Avvia un job di download per una o più ore specifiche.
    Gli input non validi (formato errato o data non plausibile) vengono ignorati.
    Se un giorno risulta completo (24 ore processate o marcate 404),
    viene automaticamente convertito in formato Parquet.
    """
    from datetime import datetime

    hours_to_process = sorted(list(set(args.hours)))
    logger.info(f"Avvio download di {len(hours_to_process)} ore specifiche...")

    total_parsed, total_distilled, total_discarded = 0, 0, 0
    valid_hours = []

    for hour_stamp in hours_to_process:
        parsed_hour = parse_hour(hour_stamp)
        if parsed_hour is None:
            logger.warning(f"Ignorata ora non valida o malformata: {hour_stamp}")
            continue
        valid_hours.append(hour_stamp)

    if not valid_hours:
        logger.warning("Nessuna ora valida da processare. Nessuna operazione eseguita.")
        return

    for hour_stamp in valid_hours:
        logger.info(f"--- Elaborazione ora: {hour_stamp} ---")
        _status, parsed, distilled, discarded = use_case.process_single_hour(hour_stamp)

        total_parsed += parsed
        total_distilled += distilled
        total_discarded += discarded

    completed_days = {datetime.strptime(h, "%Y-%m-%d-%H").date() for h in valid_hours}
    use_case.finalize_daily_indexes(list(completed_days))

    logger.info(
        f"Riepilogo delle ore valide: parsed={total_parsed}, "
        f"distilled={total_distilled}, bad={total_discarded}"
    )

def show_dataset_info(args, logger: LayerLoggerAdapter) -> None:
    """
    Mostra informazioni quantitative sul dataset locale:
    - percorso assoluto del dataset
    - dimensione totale in MB
    - ore trovate rispetto al totale teorico nel range (estremi inclusi)
    """
    base_dir = _get_dataset_path(args)
    abs_path = os.path.abspath(base_dir)
    logger.info(f"Percorso dataset: {abs_path}")

    # Caso 1: dataset inesistente
    if not os.path.exists(base_dir):
        logger.warning("Il dataset locale non esiste.")
        return

    # Caso 2: dimensione totale
    size_mb = folder_size_mb(base_dir)
    logger.info(f"Dimensione: {size_mb:.2f} MB")

    # Caso 3: riepilogo orario
    summary = get_dataset_summary(base_dir)
    if not summary:
        logger.info("Dataset vuoto o privo di ore processate.")
        return

    min_hour, max_hour, found_hours, total_hours, pct = summary

    # Caso 4: report sintetico con percentuale di copertura
    logger.info(
        f"Ore processate: {found_hours} su {total_hours} ore totali "
        f"({pct:.2f}%) nel range {min_hour} → {max_hour}"
    )

def reset_dataset(args, logger: LayerLoggerAdapter) -> None:
    """Cancella completamente il dataset locale."""
    base_dir = _get_dataset_path(args)
    if os.path.exists(base_dir):
        confirm = input(f"ATTENZIONE: stai per cancellare {os.path.abspath(base_dir)}. Continuare? (s/n): ")
        if confirm.lower() == 's':
            shutil.rmtree(base_dir)
            logger.info(f"Dataset resettato: {base_dir}")
        else:
            logger.info("Reset annullato.")
    else:
        logger.info("Nessun dataset da resettare.")

def main():
    """Entry point della CLI: configura le dipendenze e avvia i comandi."""
    load_dotenv()
    configure_logging()
    logger = LayerLoggerAdapter(logging.getLogger(__name__), {"layer": "Presentation"})

    try:
        parser = argparse.ArgumentParser(
            prog="dataset_ingestor",
            description="Gestore per il download e la distillazione di dati da GHArchive.",
        )

        parser.add_argument("--config-path", help="Cartella del Dataset (default: env DATASET_PATH o data/dataset_distillato).", default=None)
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("--download", nargs=2, metavar=("START", "END"), help="Scarica un range orario [YYYY-MM-DD-HH YYYY-MM-DD-HH].")
        group.add_argument("--hours", nargs='+', metavar="HOUR", help="Scarica ore specifiche [YYYY-MM-DD-HH ...].")
        group.add_argument("--info", action="store_true", help="Mostra info sul dataset locale.")
        group.add_argument("--reset", action="store_true", help="Cancella il dataset locale.")
        args = parser.parse_args()

        dataset_dir = _get_dataset_path(args)
        source_adapter = GhArchiveEventSource()
        repo_adapter = JsonIngestionIndexRepository(base_output_dir=dataset_dir)
        ingestion_use_case = IngestionService(
            event_source=source_adapter,
            index_repo=repo_adapter
        )

        logger.info("CLI avviata con successo.")
        if args.reset:
            return reset_dataset(args, logger)
        if args.info:
            return show_dataset_info(args, logger)
        if args.hours:
            return run_hours(args, ingestion_use_case, logger)
        if args.download:
            return run_download(args, ingestion_use_case, logger)

    except IngestionError as e:
        logger.error(f"Errore di ingestione: {e}", exc_info=False)
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Errore fatale e imprevisto: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()