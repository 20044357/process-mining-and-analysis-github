import argparse
import logging
import sys
from dotenv import load_dotenv

from ..infrastructure.logging_config import configure_logging, LayerLoggerAdapter
from ..infrastructure.gharchive_source import GhArchiveEventSource
from ..infrastructure.json_index_repository import JsonIngestionIndexRepository
from ..application.use_cases import IngestionService
from .controllers import DatasetController

def main():
    load_dotenv()
    configure_logging()

    cli_logger = LayerLoggerAdapter(logging.getLogger("CLI"), {"layer": "Presentation"})
    
    app_logger = LayerLoggerAdapter(logging.getLogger("IngestionService"), {"layer": "Application"})

    try:
        parser = argparse.ArgumentParser(description="Dataset Ingestor CLI")
        parser.add_argument("--config-path", help="Path dataset", default="data/dataset")
        
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("--download", nargs=2, metavar=("START", "END"), help="Range YYYY-MM-DD-HH")
        group.add_argument("--hours", nargs='+', metavar="HOUR", help="Lista ore YYYY-MM-DD-HH")
        group.add_argument("--info", action="store_true", help="Mostra statistiche")
        group.add_argument("--reset", action="store_true", help="Reset dataset")
        
        args = parser.parse_args()

        repo = JsonIngestionIndexRepository(args.config_path)
        source = GhArchiveEventSource()
        
        service = IngestionService(event_source=source, index_repo=repo, logger=app_logger)
        
        controller = DatasetController(service, cli_logger)

        if args.reset:
            controller.reset_dataset(args.config_path)
        elif args.info:
            controller.show_info()
        elif args.hours:
            controller.run_hours(args.hours)
        elif args.download:
            controller.run_download(args.download[0], args.download[1])

    except Exception as e:
        cli_logger.critical(f"Errore fatale imprevisto: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()