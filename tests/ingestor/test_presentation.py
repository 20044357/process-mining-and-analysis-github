import pytest
from unittest.mock import Mock
from datetime import datetime
from src.ingestor.presentation.controllers import DatasetController

class TestDatasetController:
    def test_run_download_valid(self):
        """Verifica che date valide vengano passate al service come datetime."""
        service = Mock()
        logger = Mock()
        
        service.process_time_range.return_value = (100, 90, 10) 

        controller = DatasetController(service, logger)
        
        controller.run_download("2024-01-01-10", "2024-01-01-12")
        
        service.process_time_range.assert_called_once_with(
            datetime(2024, 1, 1, 10),
            datetime(2024, 1, 1, 12)
        )

    def test_run_download_invalid_logic(self):
        """Start > End: deve loggare warning e non chiamare il service."""
        service = Mock()
        logger = Mock()
        controller = DatasetController(service, logger)
        
        controller.run_download("2024-01-01-12", "2024-01-01-10")
        
        service.process_time_range.assert_not_called()
        logger.warning.assert_called()

    def test_run_hours_mixed(self):
        """Misto valido/invalido: processa solo valido e finalizza."""
        service = Mock()
        logger = Mock()
        
        service.process_single_hour.return_value = ("SUCCESS", 10, 10, 0)

        controller = DatasetController(service, logger)
        
        controller.run_hours(["2024-01-01-10", "bad-format"])
        
        service.process_single_hour.assert_called_once_with("2024-01-01-10")
        service.finalize_daily_indexes.assert_called_once()