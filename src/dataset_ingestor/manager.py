import os
import sys
import json
from datetime import datetime, timedelta, date
from typing import Literal, Tuple
from requests import HTTPError, RequestException, Timeout, ConnectionError

from .utils import Paths, is_parquet_dataset_complete
from .sources import LineSource
from .core import distill_event
from .writers import DailyWriter
from .index import DailyIndex

ProcessHourStatus = Literal["SUCCESS", "SKIPPED", "FAILED_404", "FAILED_OTHER"]

class DatasetManager:
    """
    Gestore principale del Dataset Distillato.

    Compiti:
        - Scaricare eventi GHArchive ora per ora.
        - Distillare e scrivere eventi in JSONL.
        - Aggiornare l'indice giornaliero.
        - Convertire giornalmente in Parquet.
    """

    def __init__(
        self,
        base_output_dir: str,
        timeout: int = 60,
        retries: int = 3,
        backoff_base: float = 1.5,
    ):
        """
        Inizializza il gestore del dataset.

        Args:
            base_output_dir (str): Cartella base del dataset distillato.
            timeout (int): Timeout richieste HTTP.
            retries (int): Numero massimo di tentativi HTTP.
            backoff_base (float): Fattore base per backoff esponenziale.
        """
        self.base_output_dir = base_output_dir
        self.source = LineSource(
            timeout=timeout,
            max_retries=retries,
            backoff_base=backoff_base,
        )
        
    def process_hour(self, hour_stamp: str, force_reprocess: bool = False) -> Tuple[ProcessHourStatus, int, int, int]:
        """
        Processa un'ora, distinguendo tra successo, skip, 404 e altri errori.
        """
        day_stamp = hour_stamp[:10]
        archive_url = f"https://data.gharchive.org/{hour_stamp}.json.gz"

        paths = Paths(self.base_output_dir, day_stamp)
        daily_index = DailyIndex(paths.index_path())

        if (hour_stamp in daily_index.hours_processed or hour_stamp in daily_index.hours_not_found) and not force_reprocess:
            print(f"[SKIP] {hour_stamp} gia' presente in index.json (come processato o 404)", file=sys.stderr)
            return ("SKIPPED", 0, 0, 0)

        writer = DailyWriter(paths.events_path())
        num_parsed, num_distilled, num_discarded = 0, 0, 0

        try:
            for line in self.source.iter_lines(archive_url):
                # ... (logica interna di parsing e scrittura invariata) ...
                try:
                    raw_event = json.loads(line)
                    num_parsed += 1
                except json.JSONDecodeError:
                    num_discarded += 1
                    continue

                distilled_event = distill_event(raw_event)
                if not distilled_event:
                    num_discarded += 1
                    continue

                writer.write_event(distilled_event)
                num_distilled += 1

            writer.close_ok()
            daily_index.mark_hour(
                hour_stamp,
                {
                    "hour_total_events": num_parsed,
                    "hour_distilled_events": num_distilled,
                    "hour_bad_events": num_discarded,
                },
            )
            daily_index.save()

            print(f"[DONE] {hour_stamp}: parsed={num_parsed} distilled={num_distilled} discarded={num_discarded}", file=sys.stderr)
            return ("SUCCESS", num_parsed, num_distilled, num_discarded)

        # --- LOGICA DI GESTIONE ERRORI MIGLIORATA ---
        except HTTPError as error:
            writer.close_abort()
            if error.response.status_code == 404:
                print(f"[NOT FOUND] {archive_url}: L'archivio non esiste (404).", file=sys.stderr)
                daily_index.mark_hour_not_found(hour_stamp)
                daily_index.save()
                return ("FAILED_404", num_parsed, num_distilled, num_discarded)
            else:
                print(f"[http-error] {archive_url}: {error}", file=sys.stderr)
                return ("FAILED_OTHER", num_parsed, num_distilled, num_discarded)
        except (Timeout, ConnectionError, RequestException) as error:
            print(f"[network-error] {archive_url}: {error}", file=sys.stderr)
            writer.close_abort()
            return ("FAILED_OTHER", num_parsed, num_distilled, num_discarded)
    
    def process_range(self, start_datetime: datetime, end_datetime: datetime, force_reprocess: bool = False) -> Tuple[int, int, int]:
        # Questa funzione ora usa il nuovo stato, ma il suo corpo principale non cambia molto.
        # Le modifiche a `process_range` e `convert_day_to_parquet` della risposta precedente
        # per gestire correttamente il cambio di giorno sono corrette e le manteniamo.
        current_time = start_datetime
        total_parsed, total_distilled, total_discarded = 0, 0, 0
        previous_day = None

        while current_time <= end_datetime:
            current_day = current_time.date()
            if previous_day and current_day != previous_day:
                self.convert_day_to_parquet(previous_day)

            hour_stamp = f"{current_time.strftime('%Y-%m-%d')}-{current_time.hour}"

            # La chiamata ora restituisce anche uno stato, ma noi lo usiamo implicitamente
            # tramite l'aggiornamento dell'index_json fatto dentro process_hour.
            # Aggreghiamo solo i contatori.
            _status, parsed, distilled, discarded = self.process_hour(
                hour_stamp=hour_stamp,
                force_reprocess=force_reprocess,
            )

            total_parsed += parsed
            total_distilled += distilled
            total_discarded += discarded

            previous_day = current_day
            current_time += timedelta(hours=1)

        if previous_day:
            self.convert_day_to_parquet(previous_day)
            
        return total_parsed, total_distilled, total_discarded
        
    def convert_day_to_parquet(self, day_to_convert: date) -> None:
        """
        Converte il file JSONL di un giorno in Parquet solo se per tutte le 24 ore
        si conosce lo stato (o processata con successo o confermata come 404).
        """
        day_string: str = day_to_convert.strftime("%Y-%m-%d")
        path_manager: Paths = Paths(self.base_output_dir, day_string)
        daily_index: DailyIndex = DailyIndex(path_manager.index_path())
        
        # --- NUOVA LOGICA DI CONVERSIONE ---
        processed_count = len(daily_index.hours_processed)
        not_found_count = len(daily_index.hours_not_found)
        
        # La condizione è che la somma delle ore riuscite e quelle non trovate faccia 24.
        if processed_count + not_found_count == 24:
            # Procediamo solo se c'è effettivamente qualcosa da convertire
            if processed_count > 0 and os.path.exists(path_manager.events_path()):
                print(
                    f"[OK] Giorno completo: {day_string} ({processed_count} ore processate, "
                    f"{not_found_count} non trovate). Conversione in Parquet in corso...",
                    file=sys.stderr,
                )
                daily_writer: DailyWriter = DailyWriter(path_manager.events_path())
                daily_writer.convert_to_parquet(self.base_output_dir)
            else:
                 print(
                    f"[SKIP] Giorno {day_string} considerato completo ma senza dati da convertire "
                    f"({processed_count} ore processate).",
                    file=sys.stderr,
                )
        else:
            print(
                f"[SKIP] Giorno incompleto: {day_string}. Stato conosciuto per "
                f"{processed_count + not_found_count}/24 ore. Nessuna conversione.",
                file=sys.stderr,
            )