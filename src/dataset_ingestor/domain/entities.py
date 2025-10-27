from typing import Dict, Any, Set


class DailyIndex:
    """
    Rappresenta lo stato in memoria di un indice giornaliero (Entity del dominio).
    NON esegue alcuna operazione di I/O, ma gestisce solo la logica
    di aggiornamento dello stato.
    """

    def __init__(self, data: Dict[str, Any]):
        """
        Inizializza l'entità con i dati grezzi caricati da un repository.
        """
        self.data = data
        self.data.setdefault("hours_processed", {})
        self.data.setdefault("hours_not_found", [])
        self.data.setdefault("daily_counts", {})
        
        self.hours_processed: Set[str] = set(self.data.get("hours_processed", {}).keys())
        self.hours_not_found: Set[str] = set(self.data.get("hours_not_found", []))

    def mark_hour(self, hour_stamp: str, stats: Dict[str, int]):
        """
        Segna un'ora come processata e registra le statistiche.
        """
        self.hours_processed.add(hour_stamp)
        self.data["hours_processed"][hour_stamp] = stats

        # Se un'ora prima era 404 e ora ha successo, la rimuoviamo da not_found
        if hour_stamp in self.hours_not_found:
            self.hours_not_found.remove(hour_stamp)
            self.data["hours_not_found"] = sorted(list(self.hours_not_found))

    def mark_hour_not_found(self, hour_stamp: str):
        """Segna un'ora come non trovata (404)."""
        if hour_stamp not in self.hours_not_found:
            self.hours_not_found.add(hour_stamp)
            self.data["hours_not_found"].append(hour_stamp)
            self.data["hours_not_found"] = sorted(list(self.hours_not_found))

    def add_counts(self, new_counts: Dict[str, int]):
        """
        Aggiunge conteggi per repository all'aggregato giornaliero.
        """
        daily_counts = self.data.get("daily_counts", {})
        for key, value in new_counts.items():
            daily_counts[key] = daily_counts.get(key, 0) + value
        self.data["daily_counts"] = daily_counts