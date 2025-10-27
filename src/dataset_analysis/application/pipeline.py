# dataset_analysis/application/pipeline.py

import logging
from ..application.interfaces import IDataPrepUseCase, AnalysisMode
from ..infrastructure.logging_config import LayerLoggerAdapter

# import dei singoli casi d'uso
from .data_preparation_usecase import execute_full_data_preparation_pipeline
from .archetype_analysis_usecase import (
    execute_archetype_analysis_popular_collaborative,
    execute_archetype_analysis_collaborative_non_popular,
    execute_archetype_analysis_popular_non_collaborative,
)
from .summary_usecase import execute_summary_synthesis


class AnalysisPipeline(IDataPrepUseCase):
    """
    Application Service principale.
    Smista l’esecuzione verso i singoli casi d’uso specifici.
    """

    def __init__(self, provider, analyzer, writer, config, mode_analyzer, log_analyzer):
        self.provider = provider
        self.analyzer = analyzer
        self.writer = writer
        self.config = config
        self.model_analyzer = mode_analyzer
        self.log_analyzer = log_analyzer
        self.logger = LayerLoggerAdapter(
            logging.getLogger(self.__class__.__name__), {"layer": "Application"}
        )

    def run(self, mode: AnalysisMode):
        """Punto di ingresso unico, smista l’esecuzione dei casi d’uso."""
        self.logger.info(f"Avvio pipeline in modalità: {mode}")

        if mode == AnalysisMode.FULL:
            execute_full_data_preparation_pipeline(self.provider, self.writer, self.config, self.logger)

        elif mode == AnalysisMode.SUMMARY:
            execute_summary_synthesis(self.provider, self.writer, self.model_analyzer, self.log_analyzer, self.logger)

        elif mode == AnalysisMode.GIGANTI_POPOLARI:
            execute_archetype_analysis_popular_collaborative(self.provider, self.analyzer, self.writer, self.config, self.logger)

        elif mode == AnalysisMode.GIGANTI_NO_POPOLARI:
            execute_archetype_analysis_collaborative_non_popular(self.provider, self.analyzer, self.writer, self.config, self.logger)

        elif mode == AnalysisMode.GIGANTI_POPOLARI_NO_COLLAB:
            execute_archetype_analysis_popular_non_collaborative(self.provider, self.analyzer, self.writer, self.config, self.logger)

        else:
            raise ValueError(f"Modalità '{mode}' non supportata.")

        self.logger.info(f"Pipeline completata ({mode}).")
