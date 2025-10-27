import os
import json
import polars as pl
from typing import Dict, Any, IO

from ..domain.interfaces import IEventWriter
from .utils import Paths

class DailyEventFileWriter(IEventWriter):
    """
    Adapter che implementa IEventWriter per scrivere su file system locale.
    Gestisce un file JSONL giornaliero e la sua conversione in Parquet.
    """
    def __init__(self, paths: Paths):
        self.paths = paths
        self._file: IO | None = None
        self._open_file()

    def _open_file(self):
        """
        Apre (o riapre) il file giornaliero in modalità append.
        Crea la directory se non esiste.
        """
        dir_path = os.path.dirname(self.paths.events_path)
        os.makedirs(dir_path, exist_ok=True)

        if self._file is None or self._file.closed:
            try:
                self._file = open(self.paths.events_path, "a", encoding="utf-8")
            except Exception as e:
                print(f"[ERROR] Impossibile aprire il file {self.paths.events_path}: {e}")
                self._file = None

    def write_event(self, event: Dict[str, Any]) -> None:
        """
        Scrive un evento in formato JSONL.
        Garantisce che il file sia aperto e che il contenuto sia JSON-safe.
        """
        self._open_file()

        if not self._file or self._file.closed:
            print(f"[ERROR] File non disponibile per la scrittura: {self.paths.events_path}")
            return

        try:
            safe_event = json.dumps(event, default=str, ensure_ascii=False)
            self._file.write(safe_event + "\n")
        except Exception as e:
            print(f"[ERROR] Scrittura evento fallita su {self.paths.events_path}: {e}")

    def close_ok(self) -> None:
        if self._file and not self._file.closed:
            self._file.close()

    def close_abort(self) -> None:
        self.close_ok()

    def convert_to_parquet(self) -> None:
        self.close_ok()
        source_path = self.paths.events_path
        target_dir = self.paths.parquet_dir

        if not os.path.exists(source_path) or os.path.getsize(source_path) == 0:
            print(f"[WARN] File {source_path} vuoto o non esistente. Conversione saltata.")
            return

        try:
            df = pl.read_ndjson(source_path)
            os.makedirs(target_dir, exist_ok=True)
            df.write_parquet(os.path.join(target_dir, "events.parquet"), use_pyarrow=True, compression="zstd")

            os.remove(source_path)
            print(f"[SUCCESS] Convertito {source_path} in Parquet e rimosso l'originale.")
        except Exception as e:
            print(f"[ERROR] Impossibile convertire {source_path} in Parquet: {e}")
