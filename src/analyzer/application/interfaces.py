from abc import ABC, abstractmethod
from enum import Enum
import argparse

class AnalysisMode(Enum):
    FULL = "full"
    PROCESS_DISCOVERY = "process_discovery"
    STRUCTURAL_COMPARISON = "structural_comparison"

class IDataPrepUseCase(ABC):
    @abstractmethod
    def run(self, mode: AnalysisMode, args: argparse.Namespace):
        """
        Esegue la pipeline completa o una sua variante, a seconda della modalità.
        Deve gestire internamente il flusso:
            - Estrazione dati
            - Calcolo metriche
            - Stratificazione
            - Analisi finale
        """
        pass