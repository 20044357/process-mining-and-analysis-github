import pytest
import os
import json
from datetime import date
from src.ingestor.infrastructure.json_index_repository import JsonIngestionIndexRepository
from src.ingestor.infrastructure.file_writer import DailyEventFileWriter
from src.ingestor.infrastructure.fs_utils import Paths
from src.ingestor.domain.entities import DailyIndex, DistilledEvent

class TestJsonRepository:
    def test_save_and_load(self, tmp_path):
        repo = JsonIngestionIndexRepository(str(tmp_path))
        d = date(2024, 1, 1)

        idx = DailyIndex({})
        idx.mark_hour("2024-01-01-10", {"total": 5})
        repo.save(idx, d)

        assert (tmp_path / "2024/01/01/index.json").exists()
        
        loaded = repo.get_by_day(d)
        assert "2024-01-01-10" in loaded.hours_processed

class TestFileWriter:
    def test_write_append_mode(self, tmp_path):
        """Verifica che il writer scriva sul file events.jsonl tramite il temp."""
        paths = Paths(str(tmp_path), date(2024, 1, 1))
        writer = DailyEventFileWriter(paths)
        
        evt = DistilledEvent("id", "Push", "2024-01-01", 1, "repo")
        writer.write_event(evt)
        writer.close_ok()
        
        assert os.path.exists(paths.events_path)
        with open(paths.events_path) as f:
            line = json.loads(f.readline())
            assert line["activity"] == "Push"

    def test_consolidate_lazy(self, tmp_path):
        """Verifica che consolidate converta JSONL in Parquet e rimuova l'originale."""
        paths = Paths(str(tmp_path), date(2024, 1, 1))
        writer = DailyEventFileWriter(paths)
        
        writer.write_event(DistilledEvent("1", "Push", "2024-01-01", 1, "r"))
        writer.close_ok()
        
        assert os.path.exists(paths.events_path)
        
        writer.consolidate_storage()
        
        assert not os.path.exists(paths.events_path)
        assert os.path.exists(paths.parquet_dir + "/events.parquet")