import os
import json
from datetime import date
from typing import Dict, Any

from ..domain.entities import DailyIndex
from ..domain.interfaces import IIngestionIndexRepository, IEventWriter
from .utils import Paths
from .file_writer import DailyEventFileWriter

class JsonIngestionIndexRepository(IIngestionIndexRepository):
    """
    Adapter che implementa IIndexRepository usando file JSON su disco.
    Funziona anche come factory per il DailyFileWriter.
    """
    def __init__(self, base_output_dir: str):
        self.base_dir = base_output_dir
        self._writers_cache: Dict[date, IEventWriter] = {}

    def _get_paths(self, day: date) -> Paths:
        return Paths(base_dir=self.base_dir, day=day)

    def get_by_day(self, day: date) -> DailyIndex:
        index_path = self._get_paths(day).index_path
        if not os.path.exists(index_path):
            return DailyIndex(data={})
        
        try:
            with open(index_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return DailyIndex(data=data)
        except (json.JSONDecodeError, FileNotFoundError):
            return DailyIndex(data={})

    def save(self, index: DailyIndex, day: date) -> None:
        paths = self._get_paths(day)
        os.makedirs(paths.day_dir, exist_ok=True)
        with open(paths.index_path, "w", encoding="utf-8") as f:
            json.dump(index.data, f, indent=4)
            
    def get_writer_for_day(self, day: date) -> IEventWriter:
        """Factory method per ottenere/creare un writer per un dato giorno."""
        if day not in self._writers_cache:
            paths = self._get_paths(day)
            self._writers_cache[day] = DailyEventFileWriter(paths)
        return self._writers_cache[day]
    
    def get_parquet_path_for_day(self, day: date) -> str:
        paths = self._get_paths(day)
        return os.path.join(paths.parquet_dir, "events.parquet")
