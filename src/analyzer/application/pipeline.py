import argparse
import logging
from typing import Optional, Union 
from ..application.interfaces import IDataPrepUseCase, AnalysisMode
from ..infrastructure.logging_config import LayerLoggerAdapter 
from .data_preparation_usecase import execute_full_data_preparation_pipeline
from .archetype_analysis_usecase import execute_discover_archetype_models
from .structural_comparison_usecase import execute_structural_comparison
from .errors import InvalidInputError

class AnalysisPipeline(IDataPrepUseCase):
    def __init__(self, provider, analyzer, writer, config, mode_analyzer, logger: Optional[Union[logging.Logger, logging.LoggerAdapter]] = None):
        self.provider = provider
        self.analyzer = analyzer
        self.writer = writer
        self.config = config
        self.model_analyzer = mode_analyzer
        
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def run(self, mode: AnalysisMode, args: argparse.Namespace):
        "Punto di ingresso unico, smista l’esecuzione dei casi d’uso."
        self.logger.info(f"Avvio pipeline in modalità: {mode}")

        if mode == AnalysisMode.FULL:
            execute_full_data_preparation_pipeline(self.provider, self.writer, self.config, self.logger)

        elif mode == AnalysisMode.PROCESS_DISCOVERY:
            execute_discover_archetype_models(self.provider, self.analyzer, self.writer, self.config, self.logger)

        elif mode == AnalysisMode.STRUCTURAL_COMPARISON:
            execute_structural_comparison(self.provider, self.model_analyzer, self.writer, self.logger)

        else:
            raise ValueError(f"Modalità '{mode}' non supportata.")

        self.logger.info(f"Pipeline completata ({mode}).")