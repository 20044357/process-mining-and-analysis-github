from pathlib import Path
from ..domain import services as domain_services, predicates
from ..domain.interfaces import IDataProvider, IResultWriter
from ..config import AnalysisConfig
from ..infrastructure.logging_config import LayerLoggerAdapter
from ..domain.types import StratifiedRepositoriesDataset
from ..application.errors import (
    MissingDataError,
    DomainContractError,
    DataPreparationError,
    PipelineError,
)

def execute_full_data_preparation_pipeline(
    provider: IDataProvider,
    writer: IResultWriter,
    config: AnalysisConfig,
    logger: LayerLoggerAdapter
) -> None:
    """
    Caso d'uso: esecuzione completa della pipeline di preparazione dati.
    """
    logger.info("Avvio pipeline completa di preparazione dati...")

    try:
        # === FASE 1: Estrazione repository di creazione ===
        raw_creation_events = provider.load_raw_repo_creation_events()
        repo_lookup_table = domain_services.build_repository_creation_lookup(raw_creation_events)
        if repo_lookup_table.is_empty():
            raise MissingDataError("Nessun repository valido estratto.")

        # === FASE 2: Piano metriche ===
        base_events_lf = provider.load_core_events()
        metrics_plan_lf = domain_services.build_summary_metrics_lazy_plan(
            base_events_lf,
            repo_lookup_table,
            predicates.is_significant_pop_event,
            predicates.is_significant_eng_event,
            predicates.is_significant_collab_event,
            analysis_end_date=config.end_date
        )
        writer.write_lazy_metrics_parquet(metrics_plan_lf, Path(config.raw_metrics_file).name)

        thresholds = domain_services.compute_stratification_thresholds(metrics_plan_lf, config.QUANTILES)
        writer.write_stratification_thresholds_json(thresholds, Path(config.stratification_thresholds_file).name)

        # === FASE 3: Stratificazione ===
        stratification_plan_lf = domain_services.build_stratification_plan(metrics_plan_lf, thresholds, config.QUANTILE_LABELS)
        writer.write_lazy_metrics_parquet(stratification_plan_lf, Path(config.stratified_repositories_parquet).name)

        # === FASE 4: Analisi distribuzione ===
        stratified_df: StratifiedRepositoriesDataset = provider.load_stratified_repositories()
        if not stratified_df.is_empty():
            distribution_df = domain_services.calculate_strata_distribution(stratified_df)
            writer.write_dataframe(distribution_df, Path(config.group_distribution_file).name)
            logger.info(f"Pipeline completata: {stratified_df.height} repo in {distribution_df.height} strati.")
        else:
            logger.warning("Dataset stratificato vuoto, analisi distribuzione saltata.")

    except (DomainContractError, MissingDataError, DataPreparationError) as e:
        logger.error(f"Errore nella pipeline FULL: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.critical(f"Errore imprevisto nella pipeline FULL: {e}", exc_info=True)
        raise PipelineError("Errore critico durante la preparazione dati.")
