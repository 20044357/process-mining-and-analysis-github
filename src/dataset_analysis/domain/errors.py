"""
Definisce le eccezioni specifiche del Domain Layer.
Queste eccezioni rappresentano violazioni dei contratti logici,
errori di calcolo o condizioni anomale all’interno delle regole
di business dell’analisi.
"""

class DomainError(Exception):
    """
    Eccezione base per tutti gli errori appartenenti al Domain Layer.
    Tutte le altre eccezioni di dominio devono ereditare da questa classe.
    """
    pass


class DomainContractError(DomainError):
    """
    Sollevata quando un oggetto, un DataFrame o una struttura di dati
    viola il contratto definito dal dominio (es. colonne mancanti,
    schema errato, formato non coerente).

    Indica un errore logico, non tecnico, che impedisce la corretta
    esecuzione delle regole di business.
    """
    pass


class CalculationError(DomainError):
    """
    Sollevata quando un’operazione di calcolo, aggregazione o derivazione
    delle metriche produce un risultato anomalo, incoerente o impossibile
    da valutare (es. divisione per zero, quantili non calcolabili).

    Questa eccezione rappresenta un errore di contenuto, non di I/O.
    """
    pass
