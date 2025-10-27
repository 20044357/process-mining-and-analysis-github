import polars as pl
from typing import Dict, List

from ..domain import services as domain_services
from ..domain.errors import DomainError
from ..domain.types import SamplingReport
from ..domain.interfaces import IDataProvider, IProcessAnalyzer, IResultWriter
from ..infrastructure.logging_config import LayerLoggerAdapter
from ..application.errors import DataPreparationError, PipelineError


def run_archetype_analysis_flow(
    provider: IDataProvider,
    analyzer: IProcessAnalyzer,
    writer: IResultWriter,
    logger: LayerLoggerAdapter,
    profile_name: str,
    profile_expression: pl.Expr
) -> None:
    """
    Esegue la pipeline di analisi per un singolo archetipo.

    Questa funzione rappresenta l'orchestratore applicativo che:
      1. Carica i risultati stratificati dal provider.
      2. Seleziona le repository rappresentative dell’archetipo.
      3. Esegue l’analisi di processo su ciascun campione.
      4. Gestisce eventuali eccezioni di dominio o di preparazione dati.

    Args:
        provider: Implementazione di IDataProvider per l’accesso ai dati.
        analyzer: Implementazione di IProcessAnalyzer per l’analisi dei processi.
        writer: Implementazione di IResultWriter per la persistenza dei risultati.
        config: Oggetto AnalysisConfig con i parametri di esecuzione.
        logger: Logger adattato al layer Application.
        profile_name: Nome leggibile dell’archetipo in analisi.
        profile_expression: Espressione Polars che definisce la regola di selezione.
    """

    logger.info(f"Avvio pipeline di analisi per l’archetipo: '{profile_name}'")

    try:
        # === FASE 1/3 — Caricamento del dataset stratificato ===
        logger.info("Fase 1/3 — Caricamento del dataset stratificato...")
        stratified_df = provider.load_stratified_repositories()
        if stratified_df.is_empty():
            logger.warning(f"Nessun risultato stratificato disponibile per '{profile_name}'.")
            return

        # === FASE 2/3 — Selezione delle repository rappresentative ===
        logger.info("Fase 2/3 — Selezione delle repository rappresentative...")
        profiles_to_analyze = {profile_name: profile_expression}

        sampling_report: SamplingReport = domain_services.select_representative_repos(
            stratified_df=stratified_df,
            archetype_profiles=profiles_to_analyze,
            sampling_metric="intensita_collaborativa_cum",
        )

        sampled_repo_ids = sampling_report.sampled_repo_ids
        if not any(sampled_repo_ids.values()):
            logger.warning(f"Nessuna repository selezionata per '{profile_name}'. Flusso interrotto.")
            return

        # === FASE 3/3 — Analisi di processo sui campioni ===
        logger.info(f"Fase 3/3 — Analisi di processo per l’archetipo '{profile_name}'...")
        execute_process_analysis_on_samples(provider, analyzer, writer, logger, sampled_repo_ids)

        logger.info(f"Pipeline completata con successo per l'archetipo '{profile_name}'.")

    except (DomainError, DataPreparationError) as e:
        logger.error(
            f"PIPELINE INTERROTTA per '{profile_name}': {e}",
            exc_info=True,
        )
        raise
    except Exception as e:
        logger.critical(
            f"Errore imprevisto durante la pipeline di analisi per '{profile_name}'.",
            exc_info=True,
        )
        raise PipelineError(f"Fallimento imprevisto durante l’analisi dell’archetipo '{profile_name}'.")


def execute_process_analysis_on_samples(
    provider: IDataProvider,
    analyzer: IProcessAnalyzer,
    writer: IResultWriter,
    logger: LayerLoggerAdapter,
    sampled_repos: Dict[str, List[str]]
) -> None:
    """
    Ciclo operativo 'carica → analizza → salva' per ciascuna repository campionata.

    Args:
        provider: Adapter per l’accesso ai dati Parquet (porta di uscita).
        analyzer: Adapter per l’analisi di processo (porta di uscita).
        writer: Adapter per la scrittura dei risultati (porta di uscita).
        logger: Logger del layer Application.
        sampled_repos: Dizionario {archetipo: [repo_ids]} di repository campionate.
    """
    success_count, failure_count = 0, 0

    # Mappa piatta: {repo_id: archetype}
    repo_map = {r: a for a, ids in sampled_repos.items() for r in ids}
    if not repo_map:
        logger.warning("Nessuna repository campionata per l’analisi. Flusso terminato.")
        return

    logger.info(f"Inizio analisi di processo su {len(repo_map)} repository campionate...")

    for repo_id, archetype in repo_map.items():
        try:
            logger.info(f"Analisi repository '{repo_id}' (Archetipo: {archetype})")

            # --- Caricamento eventi ---
            repo_lazy = provider.load_repository_events(repo_id)

            # --- Analisi di processo ---
            results = analyzer.analyze_repository(repo_lazy, repo_id)

            # --- Scrittura risultati ---
            if results:
                writer.write_process_analysis_artifacts(results, repo_id, archetype, suffix="str")
                logger.info(f"Risultati salvati con successo per '{repo_id}'.")
                success_count += 1
            else:
                logger.warning(f"Nessun risultato di analisi generato per '{repo_id}'.")
                failure_count += 1

        except Exception as e:
            logger.error(f"Errore durante l’analisi della repository '{repo_id}': {e}", exc_info=False)
            failure_count += 1

    logger.info(f"Analisi di processo completata. Successi: {success_count}, Fallimenti: {failure_count}.")
