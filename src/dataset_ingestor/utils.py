import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Tuple, Any

def parse_hour(hour_string: str) -> datetime:
    """
    Converte una stringa 'YYYY-MM-DD-HH' in oggetto datetime.

    Args:
        hour_string (str): Stringa oraria.

    Returns:
        datetime: Oggetto corrispondente.

    Raises:
        ValueError: Se la stringa non rispetta il formato.
    """
    try:
        date_part, hour_part = hour_string.rsplit("-", 1)
        return datetime.strptime(f"{date_part}-{hour_part.zfill(2)}", "%Y-%m-%d-%H")
    except (ValueError, IndexError):
        raise ValueError(f"Formato orario '{hour_string}' non valido.")


def folder_size_mb(path: str) -> float:
    """
    Calcola la dimensione totale di una cartella in MB.

    Args:
        path (str): Percorso della cartella.

    Returns:
        float: Dimensione totale in MB.
    """
    total_bytes = 0
    for root, _, files in os.walk(path):
        for filename in files:
            try:
                total_bytes += os.path.getsize(os.path.join(root, filename))
            except OSError:
                pass
    return total_bytes / (1024 * 1024)


def _walk_days(base_path: str):
    """
    Itera sulla struttura di cartelle del dataset restituendo tutte le date disponibili.

    La funzione si aspetta la convenzione di directory:
        base_path/YYYY/MM/DD/

    Per ogni giorno trovato, produce una tupla (anno, mese, giorno).

    Args:
        base_path (str): Percorso base del dataset distillato.

    Yields:
        tuple[str, str, str]: Triple (anno, mese, giorno) come stringhe,
        ordinate in modo crescente.
    """
    for year in sorted([p for p in os.listdir(base_path) if p.isdigit()]):
        year_path = os.path.join(base_path, year)
        if not os.path.isdir(year_path):
            continue
        for month in sorted(os.listdir(year_path)):
            month_path = os.path.join(year_path, month)
            if not os.path.isdir(month_path):
                continue
            for day in sorted(os.listdir(month_path)):
                day_path = os.path.join(month_path, day)
                if os.path.isdir(day_path):
                    yield year, month, day


def infer_date_range(base_dir: str) -> Optional[Tuple[str, str]]:
    """
    Deduce il range orario minimo e massimo nel dataset locale.

    Args:
        base_dir (str): Percorso base del dataset.

    Returns:
        tuple | None: (ora_min, ora_max) se trovati, altrimenti None.
    """
    if not os.path.exists(base_dir):
        return None
    min_hour = max_hour = None
    for year, month, day in _walk_days(base_dir):
        index_path = os.path.join(base_dir, year, month, day, "index.json")
        if not os.path.exists(index_path):
            continue
        try:
            with open(index_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for hour in data.get("hours_processed", []):
                if len(hour) != 13:
                    continue
                min_hour = hour if (min_hour is None or hour < min_hour) else min_hour
                max_hour = hour if (max_hour is None or hour > max_hour) else max_hour
        except Exception:
            continue
    return (min_hour, max_hour) if (min_hour and max_hour) else None


def is_parquet_dataset_complete(path: str) -> bool:
    """
    Verifica se un dataset Parquet per un dato giorno è già stato creato.
    """
    return os.path.exists(path) and len(os.listdir(path)) > 0


@dataclass
class Paths:
    """
    Utility per calcolare i percorsi di cartelle/file giornalieri.

    Attributi:
        base_dir (str): Cartella base dataset.
        date_string (str): Data 'YYYY-MM-DD'.
    """
    base_dir: str
    date_string: str

    def day_dir(self) -> str:
        """
        Restituisce e crea se necessario la cartella del giorno.

        Returns:
            str: Percorso della cartella giorno.
        """
        year, month, day = self.date_string.split("-")
        dir_path = os.path.join(self.base_dir, year, month, day)
        os.makedirs(dir_path, exist_ok=True)
        return dir_path

    def events_path(self) -> str:
        """
        Restituisce il percorso del file events.jsonl per quel giorno.
        """
        return os.path.join(self.day_dir(), "events.jsonl")

    def index_path(self) -> str:
        """
        Restituisce il percorso del file index.json per quel giorno.
        """
        return os.path.join(self.day_dir(), "index.json")

    def parquet_dir(self) -> str:
        """
        Restituisce il percorso della cartella del dataset Parquet.
        """
        year, month, day = self.date_string.split("-")
        return os.path.join(self.base_dir, f"anno={year}", f"mese={month}", f"giorno={day}")
