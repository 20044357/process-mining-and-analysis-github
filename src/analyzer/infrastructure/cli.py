import argparse
import os
import sys
from dotenv import load_dotenv
import logging
from typing import Union, Optional


from ..config import AnalysisConfig
from ..application.interfaces import IDataPrepUseCase, AnalysisMode
from ..application.pipeline import AnalysisPipeline
from ..infrastructure.data_provider import ParquetDataProvider
from ..infrastructure.pm4py_analyzer import PM4PyAnalyzer
from ..infrastructure.file_writer import FileResultWriter
from ..infrastructure.model_analyzer import PM4PyModelAnalyzer
from ..infrastructure.logging_config import configure_logging, LayerLoggerAdapter
from ..application.errors import InvalidInputError, DataPreparationError, MissingDataError
from ..domain import archetypes 

def main():
    """
    Questo modulo implementa l'adattatore di ingresso (Presentation Layer)
    dell'architettura esagonale: riceve i comandi, costruisce la configurazione
    e delega l'esecuzione all'Application Layer tramite la porta IDataPrepUseCase.
    """

    configure_logging()
    
    cli_logger: Union[logging.Logger, logging.LoggerAdapter] = LayerLoggerAdapter(logging.getLogger("CLI"), {"layer": "Presentation"})
    app_logger: Union[logging.Logger, logging.LoggerAdapter] = LayerLoggerAdapter(logging.getLogger("Pipeline"), {"layer": "Application"})
    infra_provider_logger = LayerLoggerAdapter(logging.getLogger("DataProvider"), {"layer": "Infrastructure"})
    infra_writer_logger = LayerLoggerAdapter(logging.getLogger("ResultWriter"), {"layer": "Infrastructure"})

    cli_logger.info("CLI avviata con successo.")

    if os.environ.get("TESTING_MODE") != "1":
        load_dotenv()

    parser = argparse.ArgumentParser(
        prog="dataset_analyzer",
        description=(
            "Specifica la modalità di esecuzione della pipeline.\n"
            "  full                    - Esegue la pipeline completa (estrazione, metriche, stratificazione)\n"
            "  process_discovery       - Esegue la discovery di un modello aggregato per ogni archetipo definito\n"
            "  structural_comparison   - Esegue l'analisi di confronto tra i modelli di processo degli archetipi"
        ),
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        "command",
        choices=[mode.value for mode in AnalysisMode],
        help="Specifica la modalità di esecuzione della pipeline."
    )

    parser.add_argument(
        "--dataset-dir",
        default=None,
        help="Directory dei dataset sorgente in formato Parquet."
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory di destinazione per i risultati dell’analisi."
    )

    args = parser.parse_args()
    
    try:
        dataset_dir = args.dataset_dir or os.environ.get("DATASET_PATH", "data/dataset")
        output_dir = args.output_dir or os.environ.get("DATA_ANALYSIS", "data/analysis_results")
        start_date_str = os.environ.get("ANALYSIS_START_DATE")
        end_date_str = os.environ.get("ANALYSIS_END_DATE")

        if not start_date_str or not end_date_str:
            raise ValueError(
                "Variabili d'ambiente ANALYSIS_START_DATE e ANALYSIS_END_DATE non definite."
            )

        config = AnalysisConfig(
            dataset_directory=dataset_dir,
            output_directory=output_dir,
            start_date=start_date_str,
            end_date=end_date_str
        )

        cli_logger.info(f"Configurazione caricata: {config}")

    except Exception as e:
        cli_logger.error(f"Errore durante la configurazione: {e}")
        sys.exit(1)
 
    try:
        
        provider = ParquetDataProvider(
            dataset_directory=config.dataset_directory,
            start_date=config.start_date,
            end_date=config.end_date,
            analyzable_repositories_file=config.analyzable_repositories_file,
            stratified_repositories_file=config.stratified_repositories_parquet,
            output_directory=config.output_directory,
            aggregate_model_subdirectory_name = config.archetype_models_subdirectory_name,
            archetype_process_models_directory=config.archetype_process_models_directory,
            logger=infra_provider_logger 
        )
        analyzer = PM4PyAnalyzer()
        writer = FileResultWriter(config, logger=infra_writer_logger) 
        model_analyzer = PM4PyModelAnalyzer()

        cli_logger.info("Adattatori infrastrutturali inizializzati correttamente.")

    except Exception as e:
        cli_logger.critical(f"Impossibile inizializzare gli adattatori: {e}")
        sys.exit(1)

    try:
        
        pipeline: IDataPrepUseCase = AnalysisPipeline(
            provider=provider,
            analyzer=analyzer,
            writer=writer,
            config=config,
            mode_analyzer=model_analyzer,
            logger=app_logger 
        )

        execution_mode = AnalysisMode(args.command)
        cli_logger.info(f"Esecuzione pipeline in modalità '{execution_mode.value}' avviata...")
        pipeline.run(mode=execution_mode, args=args)
        cli_logger.info("Pipeline completata con successo.")

    except InvalidInputError as e:
        cli_logger.error(f"ERRORE DI INPUT: {e}")
        cli_logger.info("Operazione annullata. Controlla i parametri e riprova.")
        sys.exit(1)
        
    except (DataPreparationError, MissingDataError) as e:
        cli_logger.error(f"ERRORE NEI DATI: {e}", exc_info=False)
        sys.exit(1)
    
    except Exception as e:
        cli_logger.critical(f"Errore fatale durante l'esecuzione della pipeline: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()