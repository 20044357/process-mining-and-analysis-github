import pytest
from datetime import datetime
from src.ingestor.domain.utils import parse_hour
from src.ingestor.domain.services import extract_event_payload
from src.ingestor.domain.entities import DailyIndex

class TestDomainUtils:
    def test_parse_hour_valid_single_digit(self):
        """Accetta ore senza zero padding (es. 1)."""
        dt = parse_hour("2024-01-01-1")
        assert dt == datetime(2024, 1, 1, 1)

    def test_parse_hour_valid_double_digit(self):
        """Accetta ore a due cifre (es. 10)."""
        dt = parse_hour("2024-01-01-10")
        assert dt == datetime(2024, 1, 1, 10)

    def test_parse_hour_invalid_zero_padding(self):
        """Rifiuta ore con zero padding (es. 01) come da logica utils.py."""
        assert parse_hour("2024-01-01-01") is None

    def test_parse_hour_invalid_format(self):
        assert parse_hour("2024/01/01") is None
        assert parse_hour("bad-format") is None

    def test_parse_hour_impossible_date(self):
        """Gestisce date impossibili (es. 30 Febbraio)."""
        assert parse_hour("2024-02-30-10") is None

class TestDomainServices:
    def test_extract_payload_success(self):
        raw = {
            "type": "PushEvent",
            "created_at": "2024-01-01T10:00:00Z",
            "actor": {"id": 1, "login": "u"},
            "repo": {"id": 2, "name": "u/r"}
        }
        evt = extract_event_payload(raw)
        assert evt is not None
        assert evt.activity == "PushEvent"
        assert evt.payload_type == "Push"

    def test_extract_payload_discard_incomplete(self):
        """Se mancano campi obbligatori (es. repo), ritorna None."""
        raw = {"type": "PushEvent", "actor": {"id": 1}}
        assert extract_event_payload(raw) is None

class TestDomainEntities:
    def test_daily_index_mark_hour(self):
        idx = DailyIndex({})
        idx.mark_hour("2024-01-01-10", {"total": 100})
        assert "2024-01-01-10" in idx.hours_processed

    def test_daily_index_mark_not_found(self):
        idx = DailyIndex({})
        idx.mark_hour_not_found("2024-01-01-11")
        assert "2024-01-01-11" in idx.hours_not_found