import logging
import polars as pl
from pathlib import Path
from typing import Union
from ..domain import services as domain_services, predicates
from ..domain.interfaces import IDataProvider, IResultWriter
from ..config import AnalysisConfig
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
    logger: Union[logging.Logger, logging.LoggerAdapter] 
) -> None:
    logger.info("Avvio pipeline di preparazione dei dati per la fase di analisi...")

    try:
        
        raw_creation_events = provider.load_raw_repo_creation_events()
        repo_lookup_table = domain_services.extract_analyzable_repository(raw_creation_events)
        if repo_lookup_table.is_empty():
            raise MissingDataError("Nessun repository valido estratto.")

        analyzable_repos_df =repo_lookup_table.select("repo_id")
        writer.write_dataframe(analyzable_repos_df, Path(config.analyzable_repositories_file).name)        
        
        logger.info("Avvio calcolo delle metriche per le repository analizzabili...")
        base_events_lf = provider.load_core_events()
        metrics_plan_lf = domain_services.calculate_metrics_for_repository(
            base_events_lf,
            repo_lookup_table,
            predicates.is_significant_pop_event,
            predicates.is_significant_eng_event,
            predicates.is_significant_collab_event,
            analysis_end_date=config.end_date
        )
        writer.write_metrics_parquet(metrics_plan_lf, Path(config.raw_metrics_file).name)

        logger.info("Avvio calcolo dei quantili per ogni metrica...")
        metrics_calculated_lf = pl.scan_parquet(config.raw_metrics_file)
        thresholds = domain_services.compute_quantiles_for_metrics(metrics_calculated_lf, config.QUANTILES)
        writer.write_stratification_thresholds_json(thresholds, Path(config.stratification_thresholds_file).name)
        
        logger.info("Avvio stratificazione delle repository analizzabili...")  
        stratification_plan_lf = domain_services.classify_repository(metrics_calculated_lf, thresholds, config.QUANTILE_LABELS)
        writer.write_metrics_parquet(stratification_plan_lf, Path(config.stratified_repositories_parquet).name)
    
        logger.info("Avvio report della distribuzione delle repository analizzabili...")  
        stratified_df: StratifiedRepositoriesDataset = provider.load_stratified_repositories()
        if not stratified_df.is_empty():
            distribution_df = domain_services.report_archetype_distribution(stratified_df)
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