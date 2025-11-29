import os
import json
from datetime import date
from typing import Dict, Any

from ..domain.entities import DailyIndex
from ..domain.interfaces import IIngestionIndexRepository, IEventWriter
from .file_writer import DailyEventFileWriter
from .fs_utils import Paths, folder_size_mb, get_dataset_summary

class JsonIngestionIndexRepository(IIngestionIndexRepository):
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
        if day not in self._writers_cache:
            paths = self._get_paths(day)
            self._writers_cache[day] = DailyEventFileWriter(paths)
        return self._writers_cache[day]
    
    def get_parquet_path_for_day(self, day: date) -> str:
        paths = self._get_paths(day)
        return os.path.join(paths.parquet_dir, "events.parquet")

    def get_storage_stats(self) -> Dict[str, Any]:
        summary = get_dataset_summary(self.base_dir)
        size_mb = folder_size_mb(self.base_dir)
        
        info = {
            "path": os.path.abspath(self.base_dir),
            "size_mb": size_mb,
            "summary": None
        }
        
        if summary:
            info["summary"] = {
                "min_hour": summary[0],
                "max_hour": summary[1],
                "found": summary[2],
                "total": summary[3],
                "pct": summary[4]
            }
        return info