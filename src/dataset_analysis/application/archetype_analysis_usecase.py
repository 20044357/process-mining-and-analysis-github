import polars as pl
from ..domain import archetypes
from ..application.orchestration_helpers import run_archetype_analysis_flow
from ..domain.interfaces import IDataProvider, IProcessAnalyzer, IResultWriter
from ..config import AnalysisConfig
from ..application.errors import DataPreparationError, PipelineError
from ..infrastructure.logging_config import LayerLoggerAdapter


def execute_archetype_analysis_popular_collaborative(
    provider: IDataProvider,
    analyzer: IProcessAnalyzer,
    writer: IResultWriter,
    config: AnalysisConfig,
    logger: LayerLoggerAdapter,
) -> None:
    """
    Caso d’uso specifico: esegue la pipeline di analisi per l’archetipo
    "Giganti Collaborativi Popolari".
    """
    logger.info("Avvio pipeline di analisi per archetipo: 'Giganti Collaborativi Popolari'")

    try:
        # === FASE 1: Definizione del profilo e avvio analisi ===
        run_archetype_analysis_flow(
            provider=provider,
            analyzer=analyzer,
            writer=writer,
            logger=logger,
            profile_name="Giganti Collaborativi Popolari",
            profile_expression=archetypes.GIGANTI_COLLABORATIVI_POPOLARI,
        )
        logger.info("Pipeline completata per archetipo: 'Giganti Collaborativi Popolari'.")

    except DataPreparationError as e:
        logger.error(f"PIPELINE INTERROTTA: errore di preparazione dati per 'Giganti Collaborativi Popolari'. Causa: {e}")
        raise
    except Exception:
        logger.critical("Errore imprevisto durante la pipeline per 'Giganti Collaborativi Popolari'.", exc_info=True)
        raise PipelineError("Fallimento imprevisto durante l’analisi di 'Giganti Collaborativi Popolari'.")


def execute_archetype_analysis_collaborative_non_popular(
    provider: IDataProvider,
    analyzer: IProcessAnalyzer,
    writer: IResultWriter,
    config: AnalysisConfig,
    logger: LayerLoggerAdapter,
) -> None:
    """
    Caso d’uso specifico: esegue la pipeline di analisi per l’archetipo
    "Giganti Collaborativi Non Popolari".
    """
    logger.info("Avvio pipeline di analisi per archetipo: 'Giganti Collaborativi Non Popolari'")

    try:
        # === FASE 1: Definizione del profilo e avvio analisi ===
        run_archetype_analysis_flow(
            provider=provider,
            analyzer=analyzer,
            writer=writer,
            logger=logger,
            profile_name="Giganti Collaborativi Non Popolari",
            profile_expression=archetypes.GIGANTI_COLLABORATIVI_NON_POPOLARI,
        )
        logger.info("Pipeline completata per archetipo: 'Giganti Collaborativi Non Popolari'.")

    except DataPreparationError as e:
        logger.error(f"PIPELINE INTERROTTA: errore di preparazione dati per 'Giganti Collaborativi Non Popolari'. Causa: {e}")
        raise
    except Exception:
        logger.critical("Errore imprevisto durante la pipeline per 'Giganti Collaborativi Non Popolari'.", exc_info=True)
        raise PipelineError("Fallimento imprevisto durante l’analisi di 'Giganti Collaborativi Non Popolari'.")


def execute_archetype_analysis_popular_non_collaborative(
    provider: IDataProvider,
    analyzer: IProcessAnalyzer,
    writer: IResultWriter,
    config: AnalysisConfig,
    logger: LayerLoggerAdapter,
) -> None:
    """
    Caso d’uso specifico: esegue la pipeline di analisi per l’archetipo
    "Giganti Popolari Non Collaborativi".
    """
    logger.info("Avvio pipeline di analisi per archetipo: 'Giganti Popolari Non Collaborativi'")

    try:
        # === FASE 1: Definizione del profilo e avvio analisi ===
        run_archetype_analysis_flow(
            provider=provider,
            analyzer=analyzer,
            writer=writer,
            logger=logger,
            profile_name="Giganti Popolari Non Collaborativi",
            profile_expression=archetypes.GIGANTI_POPOLARI_NON_COLLABORATIVI,
        )
        logger.info("Pipeline completata per archetipo: 'Giganti Popolari Non Collaborativi'.")

    except DataPreparationError as e:
        logger.error(f"PIPELINE INTERROTTA: errore di preparazione dati per 'Giganti Popolari Non Collaborativi'. Causa: {e}")
        raise
    except Exception:
        logger.critical("Errore imprevisto durante la pipeline per 'Giganti Popolari Non Collaborativi'.", exc_info=True)
        raise PipelineError("Fallimento imprevisto durante l’analisi di 'Giganti Popolari Non Collaborativi'.")
