from abc import ABC, abstractmethod
from typing import Iterator, Dict, Any
from datetime import date
from .entities import DailyIndex, DistilledEvent

class IEventSource(ABC):
    @abstractmethod
    def iter_events(self, url: str) -> Iterator[Dict[str, Any]]:
        pass

class IEventWriter(ABC):
    @abstractmethod
    def write_event(self, event: DistilledEvent) -> None:
        pass

    @abstractmethod
    def close_ok(self) -> None:
        pass

    @abstractmethod
    def close_abort(self) -> None:
        pass

    @abstractmethod
    def consolidate_storage(self) -> None:
        pass

class IIngestionIndexRepository(ABC):
    @abstractmethod
    def get_by_day(self, day: date) -> DailyIndex:
        pass

    @abstractmethod
    def save(self, index: DailyIndex, day: date) -> None:
        pass

    @abstractmethod
    def get_writer_for_day(self, day: date) -> IEventWriter:
        pass

    @abstractmethod
    def get_parquet_path_for_day(self, day: date) -> str:
        pass
    
    @abstractmethod
    def get_storage_stats(self) -> Dict[str, Any]:
        pass