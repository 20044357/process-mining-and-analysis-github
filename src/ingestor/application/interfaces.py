from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import Tuple, Dict, Any
from ..domain.types import IngestionHourStatus

class IIngestionUseCase(ABC):
    @abstractmethod
    def process_single_hour(self, hour_timestamp: str, force_reprocessing: bool = False) -> Tuple[IngestionHourStatus, int, int, int]:
        pass

    @abstractmethod
    def process_time_range(self, start_datetime: datetime, end_datetime: datetime, force_reprocess: bool = False) -> Tuple[int, int, int]:
        pass

    @abstractmethod
    def finalize_daily_indexes(self, day_list: list[date]) -> None:
        pass
    
    @abstractmethod
    def get_dataset_info(self) -> Dict[str, Any]:
        pass