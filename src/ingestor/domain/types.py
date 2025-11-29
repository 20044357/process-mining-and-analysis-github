from typing import Literal

IngestionHourStatus = Literal[
    "SUCCESS",
    "SKIPPED_PROCESSED",
    "SKIPPED_404",
    "FAILED_404",
    "FAILED_OTHER"
]
"""
Stato del processo di elaborazione per una singola ora.

Valori possibili:
- SUCCESS:
    L'elaborazione dell'ora è stata completata con successo.
    L'ora è ora presente in `hours_processed` con le relative metriche.
- SKIPPED_PROCESSED:
    L'ora era già stata elaborata in precedenza con successo.
    Viene saltata per evitare duplicazioni.
- SKIPPED_404:
    L'ora era già stata marcata come non trovata (errore 404)
    in una precedente esecuzione. Viene saltata.
- FAILED_404:
    L'archivio orario non è stato trovato su GHArchive (HTTP 404)
    durante questa esecuzione. L'ora viene aggiunta a `hours_not_found`.
- FAILED_OTHER:
    Si è verificato un altro errore (rete, parsing, I/O, ecc.)
    che ha impedito l'elaborazione dell'ora corrente.
"""
