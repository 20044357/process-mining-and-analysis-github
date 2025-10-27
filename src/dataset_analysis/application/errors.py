class PipelineError(Exception):
    """Eccezione base per tutti gli errori legati alla pipeline."""
    pass

class DataPreparationError(PipelineError):
    """
    Sollevata quando un passo critico nella preparazione dei dati fallisce,
    impedendo alla pipeline di continuare.
    """
    pass

class MissingDataError(DataPreparationError):
    """
    Sollevata specificamente quando un input di dati fondamentale è mancante o vuoto.
    """
    pass

class DomainContractError(ValueError, PipelineError):
    """
    Sollevata quando una funzione del dominio riceve dati che non rispettano
    il suo contratto (es. schema del DataFrame errato).
    Indica un problema logico o di flusso dati, non un errore di I/O.
    """
    pass