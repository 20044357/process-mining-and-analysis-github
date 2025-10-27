import json
import os
from dataclasses import dataclass
from datetime import datetime, date
import re
from typing import Optional, Tuple

# ============================================================
#  Gestione percorsi e informazioni di supporto per il dataset
# ============================================================

@dataclass
class Paths:
    """Utility per calcolare i percorsi di file e cartelle giornalieri."""
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


# ============================================================
#  Utility generiche
# ============================================================

def parse_hour(hour_string: str) -> Optional[datetime]:
    """
    Converte una stringa 'YYYY-MM-DD-H' in oggetto datetime rigoroso.
    Se l'input non rispetta il formato o è una data/ora non valida, restituisce None.
    Non solleva eccezioni.
    """
    # Controllo sintattico rigoroso del formato
    if not re.match(r"^\d{4}-\d{2}-\d{2}-\d{1,2}$", hour_string):
        return None

    # Validazione semantica del contenuto
    try:
        parsed = datetime.strptime(hour_string, "%Y-%m-%d-%H")
    except ValueError:
        return None

    # Scarta date non plausibili (es. giorno 32)
    if parsed.year < 2000 or parsed.year > 2100:
        return None

    return parsed

def folder_size_mb(path: str) -> float:
    """Calcola la dimensione totale di una cartella (ricorsiva) in MB."""
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


# ============================================================
#  Analisi di copertura del dataset
# ============================================================

def get_dataset_summary(base_dir: str) -> Optional[Tuple[str, str, int, int, float]]:
    """
    Deduce il range orario (min e max) e calcola:
      - quante ore sono state effettivamente processate (trovate nei file index.json)
      - quante ore totali teoriche sono comprese nel range (estremi inclusi)
      - la percentuale di completezza del dataset

    Restituisce None se i dati non sono sufficienti o contengono orari non validi.
    """
    min_hour = max_hour = None
    found_hours: set[str] = set()

    if not os.path.isdir(base_dir):
        return None

    for year in sorted(os.listdir(base_dir)):
        year_path = os.path.join(base_dir, year)
        if not os.path.isdir(year_path) or not year.isdigit():
            continue

        for month in sorted(os.listdir(year_path)):
            month_path = os.path.join(year_path, month)
            if not os.path.isdir(month_path):
                continue

            for day in sorted(os.listdir(month_path)):
                day_path = os.path.join(month_path, day)
                if not os.path.isdir(day_path):
                    continue

                index_path = os.path.join(day_path, "index.json")
                if not os.path.exists(index_path):
                    continue

                try:
                    with open(index_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    hours = data.get("hours_processed", {})
                    if not hours:
                        continue

                    found_hours.update(hours.keys())

                    for hour in hours.keys():
                        min_hour = hour if min_hour is None else min(min_hour, hour)
                        max_hour = hour if max_hour is None else max(max_hour, hour)

                except Exception:
                    continue

    if not min_hour or not max_hour:
        return None

    start_dt = parse_hour(min_hour)
    end_dt = parse_hour(max_hour)
    if not start_dt or not end_dt:
        # range non valido → ignora
        return None

    total_possible_hours = int((end_dt - start_dt).total_seconds() // 3600) + 1

    # Filtra ore valide nel range (ignora quelle non parsabili)
    found_in_range = []
    for h in found_hours:
        parsed = parse_hour(h)
        if not parsed:
            continue
        if start_dt <= parsed <= end_dt:
            found_in_range.append(h)

    total_found_hours = len(found_in_range)
    processed_pct = (
        total_found_hours / total_possible_hours * 100
        if total_possible_hours > 0 else 0.0
    )

    return min_hour, max_hour, total_found_hours, total_possible_hours, processed_pct
