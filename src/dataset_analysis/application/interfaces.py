from abc import ABC, abstractmethod
from enum import Enum

class AnalysisMode(Enum):
    """
    Enumerazione ufficiale delle modalità di esecuzione della pipeline.
    È visibile anche agli adapter (es. CLI, API REST) ma definita nel core.
    """
    FULL = "full"
    SUMMARY = "summary"
    GIGANTI_POPOLARI = "giganti_popolari"
    GIGANTI_NO_POPOLARI = "giganti_no_popolari"
    GIGANTI_POPOLARI_NO_COLLAB = "giganti_popolari_no_collab"

class IDataPrepUseCase(ABC):
    """
    Porta Inbound principale dell'Application Layer.

    Definisce il contratto di interazione che gli Adapter Driver (CLI, API, Scheduler)
    devono usare per avviare l'elaborazione dati.
    """

    @abstractmethod
    def run(self, mode: AnalysisMode):
        """
        Esegue la pipeline completa o una sua variante, a seconda della modalità.
        Deve gestire internamente il flusso:
            - Estrazione dati
            - Calcolo metriche
            - Stratificazione
            - Analisi finale
        """
        pass
