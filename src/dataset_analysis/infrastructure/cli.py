import argparse
import os
import sys
from dotenv import load_dotenv
import logging

# === Import dei componenti architetturali ===
from ..config import AnalysisConfig
from ..application.interfaces import IDataPrepUseCase, AnalysisMode
from ..application.pipeline import AnalysisPipeline
from ..infrastructure.data_provider import ParquetDataProvider
from ..infrastructure.pm4py_analyzer import PM4PyAnalyzer
from ..infrastructure.file_writer import FileResultWriter
from ..infrastructure.model_analyzer import PM4PyModelAnalyzer
from ..infrastructure.log_analyzer import PM4PyLogAnalyzer
from ..infrastructure.logging_config import configure_logging, LayerLoggerAdapter


def main():
    """
    Questo modulo implementa l'adattatore di ingresso (Presentation Layer)
    dell'architettura esagonale: riceve i comandi, costruisce la configurazione
    e delega l'esecuzione all'Application Layer tramite la porta IDataPrepUseCase.
    """

    # --- Configurazione del logging uniforme per tutti i layer ---
    configure_logging()
    logger = LayerLoggerAdapter(logging.getLogger(__name__), {"layer": "Presentation"})
    logger.info("CLI avviata con successo.")

    # --- Caricamento variabili d’ambiente (solo in modalità normale) ---
    if os.environ.get("TESTING_MODE") != "1":
        load_dotenv()

    # --- Parser dei comandi CLI ---
    parser = argparse.ArgumentParser(
        prog="dataset_analyzer",
        description=(
            "Specifica la modalità di esecuzione della pipeline.\n"
            "  full       - Esegue la pipeline completa (estrazione, metriche, stratificazione, sintesi)\n"
            "  summary    - Aggrega i risultati delle analisi di processo\n"
            "  giganti_popolari              - Analisi archetipica: collaborativi e popolari\n"
            "  giganti_no_popolari           - Analisi archetipica: collaborativi ma non popolari\n"
            "  giganti_popolari_no_collab    - Analisi archetipica: popolari ma non collaborativi"
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
    parser.add_argument(
        "--samples",
        type=int,
        default=5,
        help="Numero di repository da campionare per ciascun gruppo (default: 5)."
    )

    args = parser.parse_args()

    # ===============================================================
    #  1. COSTRUZIONE DELLA CONFIGURAZIONE
    # ===============================================================
    try:
        dataset_dir = args.dataset_dir or os.environ.get("DATASET_PATH", "data/dataset_distillato")
        output_dir = args.output_dir or os.environ.get("DATA_ANALYSIS", "data/dataset_analyzed")
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

        logger.info(f"Configurazione caricata: {config}")

    except Exception as e:
        logger.error(f"Errore durante la configurazione: {e}")
        sys.exit(1)

    # ===============================================================
    #  2. INIEZIONE DELLE DIPENDENZE (ADAPTERS)
    # ===============================================================
    try:
        provider = ParquetDataProvider(
            dataset_directory=config.dataset_directory,
            start_date=config.start_date,
            end_date=config.end_date,
            stratified_repositories_file=config.stratified_repositories_parquet,
            output_directory=config.output_directory
        )
        analyzer = PM4PyAnalyzer()
        writer = FileResultWriter(config)
        model_analyzer = PM4PyModelAnalyzer()
        log_analyzer = PM4PyLogAnalyzer()

        logger.info("Adattatori infrastrutturali inizializzati correttamente.")

    except Exception as e:
        logger.critical(f"Impossibile inizializzare gli adattatori: {e}")
        sys.exit(1)

    # ===============================================================
    #  3. ESECUZIONE DELLA PIPELINE (Application Layer)
    # ===============================================================
    try:
        pipeline: IDataPrepUseCase = AnalysisPipeline(
            provider=provider,
            analyzer=analyzer,
            writer=writer,
            config=config,
            mode_analyzer=model_analyzer,
            log_analyzer=log_analyzer
        )

        execution_mode = AnalysisMode(args.command)
        logger.info(f"Esecuzione pipeline in modalità '{execution_mode.value}' avviata...")
        pipeline.run(mode=execution_mode)
        logger.info("Pipeline completata con successo.")

    except Exception as e:
        logger.critical(f"Errore fatale durante l'esecuzione della pipeline: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
