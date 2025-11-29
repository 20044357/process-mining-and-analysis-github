import logging
from typing import Union, Any
from ..domain import archetypes
from ..domain.interfaces import IDataProvider, IProcessAnalyzer, IResultWriter
from ..config import AnalysisConfig
from ..application.errors import MissingDataError
from ..domain.interfaces import ProcessModelArtifact 

def execute_discover_archetype_models(
    provider: IDataProvider,
    analyzer: IProcessAnalyzer,
    writer: IResultWriter,
    config: AnalysisConfig,
    logger: Union[logging.Logger, logging.LoggerAdapter] 
) -> None:
    logger.info("Avvio pipeline di process discovery...")

    defined_archetypes = archetypes.ALL_ARCHETYPES
    stratified_df = provider.load_stratified_repositories()
    if stratified_df.is_empty():
        raise MissingDataError("Dataset stratificato vuoto.")

    for name, expression in defined_archetypes.items():
        logger.info(f"Avvio analisi per l'archetipo '{name}'...")

        repo_ids = stratified_df.filter(expression).get_column("repo_id").to_list()
        if not repo_ids:
            logger.warning(f"Nessuna repository trovata per '{name}'. Salto.")
            continue
        
        logger.info(f"Trovate {len(repo_ids)} repository. Avvio ottenimento eventi...")
        all_events_lazy = provider.build_aggregates_lazyframe(repo_ids)

        logger.info(f"Avvio normalizzazione e materializzazione degli eventi...")
        all_events_log = analyzer.prepare_log(all_events_lazy)

        frequency_model: ProcessModelArtifact = analyzer.discover_heuristic_model_frequency(all_events_log)
        performance_model: ProcessModelArtifact = analyzer.discover_heuristic_model_performance(all_events_log)

        writer.save_event_log(all_events_log, archetype_name=name)
        writer.save_model_as_pickle(frequency_model, archetype_name=name, model_type="frequency")
        writer.save_model_as_pickle(performance_model, archetype_name=name, model_type="performance")
        writer.save_model_visualization(frequency_model, archetype_name=name, model_type="frequency")
        writer.save_model_visualization(performance_model, archetype_name=name, model_type="performance")