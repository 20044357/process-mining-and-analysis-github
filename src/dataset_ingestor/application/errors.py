class IngestionError(Exception):
    """Eccezione base per tutti gli errori legati alla pipeline di ingestione."""
    pass

class DataSourceError(IngestionError):
    """
    Sollevata quando la sorgente dati esterna (es. GHArchive) non è raggiungibile,
    restituisce un errore (es. 404), o si verifica un problema di rete.
    """
    pass

class InvalidInputError(IngestionError, ValueError):
    """
    Sollevata quando l'input fornito dall'utente (es. via CLI) non è valido
    e impedisce l'avvio del caso d'uso (es. date in formato errato, range invalido).
    """
    pass