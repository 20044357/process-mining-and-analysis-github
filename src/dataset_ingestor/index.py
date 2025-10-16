import os
import json
from typing import Dict, Any


class DailyIndex:
    """
    Rappresenta e gestisce l'indice giornaliero (index.json).

    L'indice tiene traccia di:
        - ore processate e relative statistiche
        - conteggi aggregati giornalieri per repository
    """

    def __init__(self, file_path: str):
        """
        Inizializza l'indice giornaliero.

        Args:
            file_path (str): Percorso del file index.json da caricare/creare.
        """
        self.file_path = file_path
        self.data = self._load()
        self.hours_processed = set(self.data.get("hours_processed", {}).keys())
        self.hours_not_found = set(self.data.get("hours_not_found", []))

    def _load(self) -> Dict[str, Any]:
        """
        Carica da disco il contenuto dell'indice giornaliero.

        Se il file indicato da `self.file_path` non esiste, inizializza una
        struttura vuota con le chiavi:
            - "hours_processed": {}  # mappa ora (YYYY-MM-DD-HH) → KPI orari
            - "daily_counts": {}     # conteggi aggregati giornalieri (es. per repo)

        Returns:
            Dict[str, Any]: Dizionario con i dati dell'indice, letti dal file
            JSON se presente oppure la struttura vuota di default.
        """
        if not os.path.exists(self.file_path):
            return {"hours_processed": {}, "hours_not_found": [], "daily_counts": {}}
        with open(self.file_path, "r") as f:
            data = json.load(f)
            data.setdefault("hours_not_found", [])
            return data

    def mark_hour(self, hour_stamp: str, stats: Dict[str, int]):
        """
        Segna un'ora come processata e registra le statistiche.

        Args:
            hour_stamp (str): Ora processata (YYYY-MM-DD-HH).
            stats (dict): KPI per l'ora (total, distilled, bad).
        """
        self.hours_processed.add(hour_stamp)
        self.data["hours_processed"][hour_stamp] = stats

        # Se un'ora prima era 404 e ora ha successo (re-run), la rimuoviamo da not_found
        if hour_stamp in self.data["hours_not_found"]:
            self.data["hours_not_found"].remove(hour_stamp)

    def mark_hour_not_found(self, hour_stamp: str):
        """Segna un'ora come non trovata (404)."""
        if hour_stamp not in self.data["hours_not_found"]:
            self.data["hours_not_found"].append(hour_stamp)
        self.hours_not_found.add(hour_stamp)

    def add_counts(self, new_counts: Dict[str, int]):
        """
        Aggiunge conteggi per repository all'aggregato giornaliero.

        Args:
            new_counts (dict): Dizionario {repo: num_eventi}.
        """
        daily_counts = self.data.get("daily_counts", {})
        for key, value in new_counts.items():
            daily_counts[key] = daily_counts.get(key, 0) + value
        self.data["daily_counts"] = daily_counts

    def save(self):
        """
        Salva l'indice aggiornato su file JSON (indentato).
        """
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        with open(self.file_path, "w") as f:
            json.dump(self.data, f, indent=4)
