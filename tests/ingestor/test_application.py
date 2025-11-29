import pytest
from unittest.mock import Mock, ANY
from datetime import date
from src.ingestor.application.use_cases import IngestionService
from src.ingestor.application.errors import DataSourceError
from src.ingestor.domain.entities import DailyIndex

@pytest.fixture
def mock_deps():
    source = Mock()
    repo = Mock()
    writer = Mock()
    
    repo.get_writer_for_day.return_value = writer
    repo.get_by_day.return_value = DailyIndex({})
    
    repo.get_parquet_path_for_day.return_value = "/fake/path/events.parquet"
    
    return source, repo, writer

class TestIngestionService:
    def test_process_hour_success(self, mock_deps):
        source, repo, writer = mock_deps
        service = IngestionService(source, repo)
        
        source.iter_events.return_value = iter([
            {"type": "PushEvent", "actor": {"id":1}, "repo": {"name":"a"}, "created_at": "d"}
        ])

        status, parsed, _, _ = service.process_single_hour("2024-01-01-10")

        assert status == "SUCCESS"
        assert parsed == 1
        writer.write_event.assert_called_once()
        repo.save.assert_called_once()

    def test_process_hour_skipped_if_processed(self, mock_deps):
        source, repo, writer = mock_deps
        repo.get_by_day.return_value = DailyIndex({"hours_processed": {"2024-01-01-10": {}}})
        
        service = IngestionService(source, repo)
        status, _, _, _ = service.process_single_hour("2024-01-01-10")

        assert status == "SKIPPED_PROCESSED"
        source.iter_events.assert_not_called()

    def test_process_hour_fail_network_error(self, mock_deps):
        """
        In caso di errore generico (es. timeout):
        1. Abortire writer
        2. NON salvare indice (così al prossimo giro riprova)
        """
        source, repo, writer = mock_deps
        service = IngestionService(source, repo)
        source.iter_events.side_effect = DataSourceError("Network Down")

        status, _, _, _ = service.process_single_hour("2024-01-01-10")

        assert status == "FAILED_OTHER"
        writer.close_abort.assert_called_once()
        repo.save.assert_not_called() 

    def test_process_hour_fail_404(self, mock_deps):
        """
        In caso di 404:
        1. Abortire writer
        2. SALVARE indice come not_found
        """
        source, repo, writer = mock_deps
        service = IngestionService(source, repo)
        
        err = DataSourceError("Not Found")
        err.__cause__ = Exception("404 Client Error")
        source.iter_events.side_effect = err

        status, _, _, _ = service.process_single_hour("2024-01-01-10")

        assert status == "FAILED_404"
        writer.close_abort.assert_called_once()
        repo.save.assert_called_once()
        
        saved_idx = repo.save.call_args[0][0]
        assert "2024-01-01-10" in saved_idx.hours_not_found

    def test_consolidate_triggered(self, mock_deps):
        """Se il giorno è completo (23 processed + 1 corrente), avvia consolidamento."""
        source, repo, writer = mock_deps
        service = IngestionService(source, repo)
        
        repo.get_parquet_path_for_day.return_value = "/path/not/exists.parquet"
        
        fake_data = {"hours_processed": {f"2024-01-01-{h:02d}": {} for h in range(23)}}

        idx_complete = DailyIndex(fake_data.copy())
        idx_complete.mark_hour("2024-01-01-23", {})
        
        repo.get_by_day.return_value = idx_complete

        service.finalize_daily_indexes([date(2024, 1, 1)])
        
        writer.consolidate_storage.assert_called_once()