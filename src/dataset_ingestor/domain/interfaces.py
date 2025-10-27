# File: src/dataset_ingestor/domain/interfaces.py

from abc import ABC, abstractmethod
from typing import Iterator, Dict, Any, Tuple
from datetime import date

from .entities import DailyIndex

class IEventSource(ABC):
    """Porta di uscita per accedere a una sorgente di eventi (es. GHArchive)."""
    @abstractmethod
    def iter_lines(self, url: str) -> Iterator[str]:
        pass

class IEventWriter(ABC):
    """Porta di uscita per scrivere eventi distillati e gestire la finalizzazione."""
    @abstractmethod
    def write_event(self, event: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def close_ok(self) -> None:
        pass

    @abstractmethod
    def close_abort(self) -> None:
        pass

    @abstractmethod
    def convert_to_parquet(self) -> None:
        pass

class IIngestionIndexRepository(ABC):
    """Porta di uscita per la persistenza dell'indice giornaliero."""
    @abstractmethod
    def get_by_day(self, day: date) -> DailyIndex:
        pass

    @abstractmethod
    def save(self, index: DailyIndex, day: date) -> None:
        pass

    @abstractmethod
    def get_writer_for_day(self, day: date) -> IEventWriter:
        """Restituisce un writer associato a un giorno specifico."""
        pass

    @abstractmethod
    def get_parquet_path_for_day(self, day: date) -> str:
        """Restituisce il percorso assoluto del file Parquet atteso per un dato giorno."""
        pass