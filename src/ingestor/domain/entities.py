from dataclasses import dataclass
from typing import Dict, Any, Set

@dataclass(frozen=True)
class DistilledEvent:
    case_id: str
    activity: str
    timestamp: str
    actor_id: int
    repo_name: str
    payload_type: str | None = None
    repo_id: int | None = None
    actor_login: str | None = None

class DailyIndex:
    def __init__(self, data: Dict[str, Any]):
        self.data = data
        self.data.setdefault("hours_processed", {})
        self.data.setdefault("hours_not_found", [])
        self.data.setdefault("daily_counts", {})
        
        self.hours_processed: Set[str] = set(self.data.get("hours_processed", {}).keys())
        self.hours_not_found: Set[str] = set(self.data.get("hours_not_found", []))

    def mark_hour(self, hour_stamp: str, stats: Dict[str, int]) -> None:
        self.hours_processed.add(hour_stamp)
        self.data["hours_processed"][hour_stamp] = stats

        if hour_stamp in self.hours_not_found:
            self.hours_not_found.remove(hour_stamp)
            self.data["hours_not_found"] = sorted(list(self.hours_not_found))

    def mark_hour_not_found(self, hour_stamp: str) -> None:
        """Segna un'ora come non trovata (404)."""
        if hour_stamp not in self.hours_not_found:
            self.hours_not_found.add(hour_stamp)
            self.data["hours_not_found"].append(hour_stamp)
            self.data["hours_not_found"] = sorted(list(self.hours_not_found))

    def add_counts(self, new_counts: Dict[str, int]) -> None:
        daily_counts = self.data.get("daily_counts", {})
        for key, value in new_counts.items():
            daily_counts[key] = daily_counts.get(key, 0) + value
        self.data["daily_counts"] = daily_counts