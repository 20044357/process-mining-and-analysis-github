import os
import json
from dataclasses import dataclass
from datetime import date
from typing import Optional, Tuple
from ..domain.utils import parse_hour

@dataclass
class Paths:
    base_dir: str
    day: date

    @property
    def day_dir(self) -> str:
        dir_path = os.path.join(self.base_dir, self.day.strftime('%Y/%m/%d'))
        os.makedirs(dir_path, exist_ok=True)
        return dir_path

    @property
    def events_path(self) -> str:
        return os.path.join(self.day_dir, "events.jsonl")

    @property
    def index_path(self) -> str:
        return os.path.join(self.day_dir, "index.json")

    @property
    def parquet_dir(self) -> str:
        return os.path.join(
            self.base_dir,
            f"anno={self.day.year}",
            f"mese={self.day.month:02d}",
            f"giorno={self.day.day:02d}"
        )

def folder_size_mb(path: str) -> float:
    total_bytes = 0
    if not os.path.exists(path):
        return 0.0
    for root, _, files in os.walk(path):
        for filename in files:
            try:
                total_bytes += os.path.getsize(os.path.join(root, filename))
            except OSError:
                pass
    return total_bytes / (1024 * 1024)

def get_dataset_summary(base_dir: str) -> Optional[Tuple[str, str, int, int, float]]:
    min_hour = max_hour = None
    found_hours: set[str] = set()

    if not os.path.isdir(base_dir):
        return None

    for root, _, files in os.walk(base_dir):
        if "index.json" in files:
            try:
                with open(os.path.join(root, "index.json"), "r", encoding="utf-8") as f:
                    data = json.load(f)
                    hours = data.get("hours_processed", {})
                    if hours:
                        found_hours.update(hours.keys())
            except Exception:
                continue

    if not found_hours:
        return None

    valid_dts = []
    for h in found_hours:
        dt = parse_hour(h)
        if dt:
            valid_dts.append(dt)
            
    if not valid_dts:
        return None

    min_dt, max_dt = min(valid_dts), max(valid_dts)
    total_possible_hours = int((max_dt - min_dt).total_seconds() // 3600) + 1
    
    count_in_range = 0
    for dt in valid_dts:
        if min_dt <= dt <= max_dt:
            count_in_range += 1

    processed_pct = (count_in_range / total_possible_hours * 100) if total_possible_hours > 0 else 0.0

    return (
        min_dt.strftime("%Y-%m-%d-%H"),
        max_dt.strftime("%Y-%m-%d-%H"),
        count_in_range,
        total_possible_hours,
        processed_pct
    )