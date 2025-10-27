# File: src/dataset_analysis/infrastructure/log_analyzer.py

import pm4py
import pandas as pd
import statistics
import logging
from typing import List, Optional
from pm4py.objects.log.obj import EventLog

from ..domain.interfaces import ILogAnalyzer
from ..infrastructure.logging_config import LayerLoggerAdapter


class PM4PyLogAnalyzer(ILogAnalyzer):
    """
    Adapter del layer Infrastructure che implementa la porta di uscita ILogAnalyzer.
    Integra la libreria PM4Py per analizzare log comportamentali e calcolare
    indicatori quantitativi di processo (KPI) direttamente da un EventLog.
    """

    def __init__(self):
        """Inizializza il sistema di logging dedicato al LogAnalyzer."""
        base_logger = logging.getLogger(self.__class__.__name__)
        self.logger = LayerLoggerAdapter(base_logger, {"layer": "Infrastructure"})

    # =====================================================================
    # CARICAMENTO LOG
    # =====================================================================

    def load_log_from_file(self, file_path: str) -> Optional[EventLog]:
        """
        Carica un log di eventi da file XES o da formato equivalente.
        Ritorna None in caso di errore o formato non riconosciuto.
        """
        try:
            log_object = pm4py.read_xes(file_path)

            # Conversione automatica se PM4Py restituisce un DataFrame
            if isinstance(log_object, pd.DataFrame):
                log_event = pm4py.convert_to_event_log(log_object)
                self.logger.info(f"Log convertito da DataFrame a EventLog: {file_path}")
                return log_event

            if isinstance(log_object, EventLog):
                self.logger.info(f"Log XES caricato correttamente da {file_path}.")
                return log_object

            self.logger.warning(f"Il file {file_path} non contiene un EventLog valido.")
            return None

        except FileNotFoundError:
            self.logger.warning(f"File log non trovato: {file_path}")
            return None
        except Exception as e:
            self.logger.error(f"Errore durante il caricamento del log da {file_path}: {e}", exc_info=False)
            return None

    # =====================================================================
    # KPI DI ATTIVITÀ E PARTECIPAZIONE
    # =====================================================================

    def get_num_active_actors(self, log: EventLog) -> Optional[int]:
        """
        [KPI #7] Numero di attori attivi.
        Corrisponde al numero di tracce (casi) presenti nel log.
        """
        if not log:
            return None
        try:
            return len(log)
        except Exception as e:
            self.logger.warning(f"Impossibile calcolare il numero di attori attivi: {e}")
            return None

    def get_activity_concentration(self, log: EventLog) -> Optional[float]:
        """
        [KPI #8] Indice di concentrazione dell'attività.
        Misura la proporzione di eventi generati dall’attore più attivo
        sul totale degli eventi registrati (range: 0–1).
        """
        if not log or len(log) == 0:
            return None
        try:
            num_eventi_totali = sum(len(trace) for trace in log)
            if num_eventi_totali == 0:
                return None

            eventi_attore_piu_attivo = max(len(trace) for trace in log)
            return eventi_attore_piu_attivo / num_eventi_totali

        except Exception as e:
            self.logger.warning(f"Errore durante il calcolo della concentrazione attività: {e}")
            return None

    # =====================================================================
    # KPI TEMPORALI
    # =====================================================================

    def _calculate_median_time_between(
        self, log: EventLog, start_activity: str, end_activity: str
    ) -> Optional[float]:
        """
        Calcola il tempo mediano (in secondi) trascorso tra la prima occorrenza
        di una 'start_activity' e la successiva 'end_activity' in ciascuna traccia.
        Restituisce None se non è possibile calcolare la metrica.
        """
        if not log:
            return None

        durations: List[float] = []

        for trace in log:
            if not trace:
                continue

            try:
                start_indices = [
                    i for i, event in enumerate(trace)
                    if event.get("concept:name") == start_activity
                ]
                end_indices = [
                    i for i, event in enumerate(trace)
                    if event.get("concept:name") == end_activity
                ]

                if not start_indices or not end_indices:
                    continue

                for start_idx in start_indices:
                    valid_end_idx = next((ei for ei in end_indices if ei > start_idx), None)
                    if valid_end_idx is None:
                        continue

                    start_time = trace[start_idx].get("time:timestamp")
                    end_time = trace[valid_end_idx].get("time:timestamp")

                    if start_time and end_time:
                        duration = (end_time - start_time).total_seconds()
                        if duration >= 0:
                            durations.append(duration)
            except Exception:
                continue

        if not durations:
            return None
        return statistics.median(durations)

    def get_true_pr_lead_time_hours(self, log: EventLog) -> Optional[float]:
        """
        [KPI #6, Vero] Lead Time Totale di una Pull Request (in ore).
        Calcola la mediana dei tempi tra apertura ('PullRequestEvent_opened')
        e merge ('PullRequestEvent_merged').
        """
        time_seconds = self._calculate_median_time_between(
            log,
            start_activity="PullRequestEvent_opened",
            end_activity="PullRequestEvent_merged",
        )
        return time_seconds / 3600.0 if time_seconds is not None else None

    # =====================================================================
    # KPI AGGIUNTIVI
    # =====================================================================

    def get_fork_to_pr_conversion_rate(self, log: EventLog) -> Optional[float]:
        """
        [KPI #3] Tasso di conversione da Fork a Pull Request.
        ATTENZIONE: richiede un log non filtrato contenente eventi di tipo 'ForkEvent'.
        In questa versione, la funzione restituisce None se il calcolo non è possibile.
        """
        self.logger.warning("get_fork_to_pr_conversion_rate non implementato completamente.")
        return None
