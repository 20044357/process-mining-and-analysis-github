from typing import List

# Importa le interfacce e le strutture dati del dominio/applicazione
from ..domain.interfaces import IDataProvider, IResultWriter, IModelAnalyzer, ILogAnalyzer
from ..domain.types import KpiSummaryRecord  # <-- IMPORTA LA DATACLASS
from ..application.errors import DataPreparationError, PipelineError
from ..infrastructure.logging_config import LayerLoggerAdapter


def execute_summary_synthesis(
    provider: IDataProvider,
    writer: IResultWriter,
    model_analyzer: IModelAnalyzer,
    log_analyzer: ILogAnalyzer,
    logger: LayerLoggerAdapter,
) -> None:
    """
    Caso d’uso: aggregazione dei KPI dagli artefatti di analisi.
    Questo orchestratore è agnostico rispetto a formati di file e librerie dati.
    """
    logger.info("Avvio pipeline di sintesi quantitativa dei risultati...")

    try:
        # === FASE 1: Scansione artefatti (invariata) ===
        logger.info("Fase 1/3 — Ricerca degli artefatti...")
        artifacts = provider.scan_analysis_artifacts()
        if not artifacts:
            logger.warning("Nessun artefatto trovato.")
            return

        # === FASE 2: Estrazione dei KPI in una struttura dati di dominio ===
        all_kpis: List[KpiSummaryRecord] = [] # La lista ora è tipizzata con la dataclass
        logger.info(f"Fase 2/3 — Estrazione KPI da {len(artifacts)} set di artefatti...")
        
        for art in artifacts:
            logger.info(f"→ Elaborazione artefatti di {art.repo_id} ({art.archetype_name})")
            freq_model = model_analyzer.load_model_from_file(art.freq_model_path)
            perf_model = model_analyzer.load_model_from_file(art.perf_model_path)
            log = log_analyzer.load_log_from_file(art.log_path)

            if not (freq_model and perf_model and log):
                logger.warning(f"Artefatti incompleti per {art.repo_id}.")
                continue

            try:
                record = KpiSummaryRecord(
                    repo_id=art.repo_id,
                    archetype=art.archetype_name,
                    num_attori_attivi=log_analyzer.get_num_active_actors(log),
                    activity_concentration=log_analyzer.get_activity_concentration(log),
                    pr_lead_time_total_hours=log_analyzer.get_true_pr_lead_time_hours(log),
                    is_issue_driven=model_analyzer.is_issue_driven(freq_model),
                    has_review_loop=model_analyzer.has_review_loop(freq_model),
                    rework_intensity=model_analyzer.get_rework_intensity(freq_model),
                    pr_success_rate=model_analyzer.get_pr_success_rate(freq_model),
                    num_activities=model_analyzer.count_activities(freq_model),
                    num_connections=model_analyzer.count_connections(freq_model),
                    issue_reaction_time_hours=model_analyzer.get_issue_reaction_time_hours(perf_model),
                    direct_merge_latency_hours=model_analyzer.get_direct_merge_latency_hours(perf_model),
                )
                all_kpis.append(record)

            except Exception as e:
                logger.error(f"Errore durante il calcolo KPI per {art.repo_id}: {e}", exc_info=False)

        # === FASE 3: Salvataggio tramite chiamata semantica ===
        if not all_kpis:
            logger.warning("Nessun KPI estratto.")
            return

        # NESSUN RIFERIMENTO A POLARS. NESSUN NOME DI FILE.
        # Passiamo la lista di oggetti di dominio all'adattatore.
        writer.write_quantitative_summary(all_kpis)
        
        logger.info(f"Pipeline di sintesi completata. KPI estratti per {len(all_kpis)} repository.")

    except DataPreparationError as e:
        logger.error(f"PIPELINE INTERROTTA: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.critical("Errore imprevisto nella pipeline di sintesi.", exc_info=True)
        raise PipelineError("Fallimento imprevisto nella sintesi dei KPI.") from e