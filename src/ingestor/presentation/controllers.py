from datetime import timedelta, datetime
import os
import shutil
import logging
from typing import List
from ..application.interfaces import IIngestionUseCase
from ..domain.utils import parse_hour

class DatasetController:
    def __init__(self, use_case: IIngestionUseCase, logger: logging.LoggerAdapter):
        self.use_case = use_case
        self.logger = logger

    def run_download(self, start_str: str, end_str: str):
        start_time = parse_hour(start_str)
        end_time = parse_hour(end_str)

        if not start_time or not end_time:
            self.logger.warning("Intervallo orario non valido.")
            return

        if end_time < start_time + timedelta(hours=1):
            self.logger.warning("Intervallo troppo corto (< 1h).")
            return

        self.logger.info(f"Avvio download del range: {start_time} -> {end_time}")
        total, distilled, discarded = self.use_case.process_time_range(start_time, end_time)
        self.logger.info(f"Riepilogo: Parsed={total}, Distilled={distilled}, Bad={discarded}")

    def run_hours(self, hours: List[str]):
        hours_to_process = sorted(list(set(hours)))
        valid_hours = []

        for h in hours_to_process:
            if parse_hour(h):
                valid_hours.append(h)
            else:
                self.logger.warning(f"Ignorata ora non valida: {h}")

        if not valid_hours:
            self.logger.warning("Nessuna ora valida da processare.")
            return
        
        self.logger.info(f"Avvio elaborazione di {len(valid_hours)} ore.")
        total_parsed, total_distilled, total_discarded = 0, 0, 0
        
        for hour_stamp in valid_hours:
            self.logger.info(f"Elaborazione ora: {hour_stamp}")
            _, parsed, distilled, discarded = self.use_case.process_single_hour(hour_stamp)
            total_parsed += parsed
            total_distilled += distilled
            total_discarded += discarded
        
        completed_days = {datetime.strptime(h, "%Y-%m-%d-%H").date() for h in valid_hours}
        self.use_case.finalize_daily_indexes(list(completed_days))
        
        self.logger.info(f"Totale Sessione: Parsed={total_parsed}, Distilled={total_distilled}, Bad={total_discarded}")

    def show_info(self):
        info = self.use_case.get_dataset_info()
        self.logger.info(f"Percorso dataset: {info['path']}")
        self.logger.info(f"Dimensione: {info['size_mb']:.2f} MB")
        
        summ = info.get("summary")
        if summ:
            self.logger.info(
                f"Copertura: {summ['found']} ore su {summ['total']} "
                f"({summ['pct']:.2f}%) nel range {summ['min_hour']} -> {summ['max_hour']}"
            )
        else:
            self.logger.info("Dataset vuoto o privo di indici.")

    def reset_dataset(self, path: str):
        if os.path.exists(path):
            confirm = input(f"ATTENZIONE: cancellare {path}? (s/n): ")
            if confirm.lower() == 's':
                shutil.rmtree(path)
                self.logger.info("Dataset resettato.")
            else:
                self.logger.info("Operazione annullata.")
        else:
            self.logger.info("Nessun dataset da resettare.")