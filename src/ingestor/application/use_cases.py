import logging
from datetime import datetime, timedelta, date
import os
from typing import Tuple, Dict, Any, Optional, Union

from .interfaces import IIngestionUseCase
from ..domain.interfaces import IEventSource, IIngestionIndexRepository
from ..domain import services as domain_services
from ..domain.types import IngestionHourStatus
from ..application.errors import DataSourceError

class IngestionService(IIngestionUseCase):    
    def __init__(
        self,
        event_source: IEventSource,
        index_repo: IIngestionIndexRepository,
        logger: Optional[Union[logging.Logger, logging.LoggerAdapter]] = None
    ):
        self.source = event_source
        self.index_repo = index_repo
        self.logger = logger or logging.getLogger(__name__)

    def process_single_hour(self, hour_timestamp: str, force_reprocessing: bool = False) -> Tuple[IngestionHourStatus, int, int, int]:
        hour_dt = datetime.strptime(hour_timestamp, "%Y-%m-%d-%H")
        current_day = hour_dt.date()
        archive_url = f"https://data.gharchive.org/{hour_timestamp}.json.gz"

        daily_index = self.index_repo.get_by_day(current_day)

        if hour_timestamp in daily_index.hours_processed and not force_reprocessing:
            self.logger.info(f"Ora {hour_timestamp} già elaborata con successo. Salto.")
            return "SKIPPED_PROCESSED", 0, 0, 0

        if hour_timestamp in daily_index.hours_not_found and not force_reprocessing:
            self.logger.info(f"Ora {hour_timestamp} già marcata come 404. Salto.")
            return "SKIPPED_404", 0, 0, 0

        writer = self.index_repo.get_writer_for_day(current_day)
        num_parsed, num_distilled, num_discarded = 0, 0, 0

        try:
            for raw_event in self.source.iter_events(archive_url):
                num_parsed += 1
                distilled_event = domain_services.extract_event_payload(raw_event)
                
                if not distilled_event:
                    num_discarded += 1
                    continue

                writer.write_event(distilled_event)
                num_distilled += 1

            writer.close_ok()
            daily_index.mark_hour(
                hour_timestamp,
                {"total": num_parsed, "distilled": num_distilled, "bad": num_discarded},
            )
            self.index_repo.save(daily_index, current_day)
            self.logger.info(
                f"Ora {hour_timestamp} completata: total={num_parsed}, "
                f"distilled={num_distilled}, bad={num_discarded}"
            )
            return "SUCCESS", num_parsed, num_distilled, num_discarded

        except DataSourceError as error:
            writer.close_abort()
            is_404 = '404' in str(error.__cause__) if error.__cause__ else False

            if is_404:
                self.logger.warning(f"Archivio per l'ora {hour_timestamp} non trovato (404).")
                daily_index.mark_hour_not_found(hour_timestamp)
                self.index_repo.save(daily_index, current_day)
                return "FAILED_404", 0, 0, 0
            else:
                self.logger.error(f"Errore di rete/sorgente dati per l'ora {hour_timestamp}: {error}")
                return "FAILED_OTHER", num_parsed, num_distilled, num_discarded
            
    def process_time_range(self, start_datetime: datetime, end_datetime: datetime, force_reprocess: bool = False) -> Tuple[int, int, int]:
        current_time = start_datetime
        total_parsed, total_distilled, total_discarded = 0, 0, 0
        previous_day = None

        while current_time <= end_datetime:
            current_day = current_time.date()
            if previous_day and current_day != previous_day:
                self._convert_day_if_complete(previous_day)

            hour_stamp = f"{current_time.strftime('%Y-%m-%d')}-{current_time.hour}"

            _status, parsed, distilled, discarded = self.process_single_hour(
                hour_timestamp=hour_stamp, force_reprocessing=force_reprocess
            )

            total_parsed += parsed
            total_distilled += distilled
            total_discarded += discarded

            previous_day = current_day
            current_time += timedelta(hours=1)

        if previous_day:
            self._convert_day_if_complete(previous_day)
            
        return total_parsed, total_distilled, total_discarded
        
    def _convert_day_if_complete(self, day_to_convert: date):
        daily_index = self.index_repo.get_by_day(day_to_convert)
        processed_count = len(daily_index.hours_processed)
        not_found_count = len(daily_index.hours_not_found)
        day_string = day_to_convert.strftime('%Y-%m-%d')

        parquet_path = self.index_repo.get_parquet_path_for_day(day_to_convert)
        if os.path.exists(parquet_path):
            self.logger.info(f"Giorno {day_string} già convertito in Parquet.")
            return

        if processed_count + not_found_count == 24:
            if processed_count > 0:
                self.logger.info(f"Giorno {day_string} completo. Avvio consolidamento storage...")
                writer = self.index_repo.get_writer_for_day(day_to_convert)
                writer.consolidate_storage() 
            else:
                self.logger.info(f"Giorno {day_string} completo ma vuoto.")
        else:
            self.logger.info(f"Giorno {day_string} incompleto ({processed_count + not_found_count}/24). Consolidamento rimandato.")

    def finalize_daily_indexes(self, day_list: list[date]) -> None:
        for day_to_convert in day_list:
            try:
                self._convert_day_if_complete(day_to_convert)
            except Exception as e:
                self.logger.warning(f"Consolidamento storage saltato per {day_to_convert}: {e}")

    def get_dataset_info(self) -> Dict[str, Any]:
        return self.index_repo.get_storage_stats()