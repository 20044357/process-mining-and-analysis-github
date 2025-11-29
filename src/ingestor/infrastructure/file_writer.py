import os
import json
import polars as pl
from typing import IO
from ..domain.interfaces import IEventWriter
from ..domain.entities import DistilledEvent
from .fs_utils import Paths

class DailyEventFileWriter(IEventWriter):
    def __init__(self, paths: Paths):
        self.paths = paths
        self._file: IO | None = None
        self._open_file()

    def _open_file(self):
        dir_path = os.path.dirname(self.paths.events_path)
        os.makedirs(dir_path, exist_ok=True)

        if self._file is None or self._file.closed:
            try:
                self._file = open(self.paths.events_path, "a", encoding="utf-8")
            except Exception as e:
                (f"[ERROR] Impossibile aprire {self.paths.events_path}: {e}")
                self._file = None

    def write_event(self, event: DistilledEvent) -> None:
        self._open_file()
        if not self._file or self._file.closed:
            return

        try:
            safe_event = json.dumps(event.__dict__, default=str, ensure_ascii=False)
            self._file.write(safe_event + "\n")
        except Exception as e:
            (f"[ERROR] Scrittura evento fallita: {e}")

    def close_ok(self) -> None:
        if self._file and not self._file.closed:
            self._file.close()

    def close_abort(self) -> None:
        self.close_ok()

    def consolidate_storage(self) -> None:
            if self._file and not self._file.closed:
                self._file.close()
                
            source_path = self.paths.events_path
            target_dir = self.paths.parquet_dir

            if not os.path.exists(source_path) or os.path.getsize(source_path) == 0:
                return

            try:
                os.makedirs(target_dir, exist_ok=True)
                output_path = os.path.join(target_dir, "events.parquet")

                lazy_df = pl.scan_ndjson(source_path)
                lazy_df.sink_parquet(output_path, compression="zstd")

                os.remove(source_path)
            except Exception as e:
                (f"[ERROR] Consolidamento streaming fallito per {source_path}: {e}")