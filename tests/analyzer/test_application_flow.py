from datetime import datetime, timezone
from unittest.mock import Mock, ANY, patch
import pytest
import polars as pl
import logging
from src.ingestor.application.use_cases import IngestionService 
from src.analyzer.application.errors import MissingDataError
from src.analyzer.application.pipeline import AnalysisPipeline, AnalysisMode
from src.analyzer.domain.interfaces import IDataProvider, IResultWriter, IProcessAnalyzer, IModelAnalyzer
from src.analyzer.config import AnalysisConfig
from src.analyzer.domain.types import StratifiedRepositoriesDataset

@pytest.fixture
def mock_logger():
    
    return logging.getLogger("TestLogger")

@pytest.fixture
def mock_config(tmp_path):
    return AnalysisConfig(
        dataset_directory="/fake/data", 
        output_directory=str(tmp_path),
        start_date="2023-01-01",
        end_date="2023-01-31"
    )

@pytest.fixture
def mock_pipeline_deps(mock_config):
    provider = Mock(spec=IDataProvider)
    analyzer = Mock(spec=IProcessAnalyzer)
    writer = Mock(spec=IResultWriter)
    model_analyzer = Mock(spec=IModelAnalyzer)
    
    provider.load_raw_repo_creation_events.return_value = pl.DataFrame({
        "repo_id": [1], 
        "timestamp": [datetime(2023, 1, 1, tzinfo=timezone.utc)] 
    })
     
    core_events_data = {
        "repo_id": [1], 
        "activity": ["PushEvent"], 
        "actor_id": [1],
        "actor_login": ["user_a"], 
        "push_size": [10],         
        "action": [None],
        "timestamp": [datetime(2023, 1, 15, tzinfo=timezone.utc)] 
    }
     
    provider.load_core_events.return_value = pl.LazyFrame(
        core_events_data,
        schema={
            "repo_id": pl.Int64,
            "activity": pl.Utf8,
            "actor_id": pl.Int64,
            "actor_login": pl.Utf8,
            "push_size": pl.Int64,
            "action": pl.Utf8,
            "timestamp": pl.Datetime(time_unit='us', time_zone='UTC'), 
        }
    )
    
    provider.load_stratified_repositories.return_value = pl.DataFrame({"repo_id": [1], "strato_id": ["S1"]})

    return provider, analyzer, writer, model_analyzer

def test_pipeline_full_success_flow(mock_pipeline_deps, mock_config, mock_logger):
    provider, analyzer, writer, model_analyzer = mock_pipeline_deps
    
    pipeline = AnalysisPipeline(provider, analyzer, writer, mock_config, model_analyzer, mock_logger)
    
    metrics_schema_cols = [
        "workload_cum", "collaboration_intensity_cum", 
        "community_engagement_cum", "external_popularity_cum",
        "workload_norm", "collaboration_intensity_norm", 
        "community_engagement_norm", "external_popularity_norm"
    ]
    
    dummy_metrics_lf = pl.DataFrame(
        {col: [10.0, 20.0] for col in metrics_schema_cols}
    ).lazy()

    with patch("polars.scan_parquet", return_value=dummy_metrics_lf):
        pipeline.run(AnalysisMode.FULL, args=Mock())
    
    provider.load_raw_repo_creation_events.assert_called_once()
    writer.write_dataframe.assert_called() 
    
    writer.write_metrics_parquet.assert_called() 
    writer.write_stratification_thresholds_json.assert_called_once()
    
    provider.load_stratified_repositories.assert_called_once()

def test_pipeline_full_fails_on_missing_repo_creation(mock_pipeline_deps, mock_config, mock_logger):
    provider, analyzer, writer, model_analyzer = mock_pipeline_deps
    
    provider.load_raw_repo_creation_events.return_value = pl.DataFrame(schema={"repo_id": pl.Int64, "timestamp": pl.Datetime})
    
    pipeline = AnalysisPipeline(provider, analyzer, writer, mock_config, model_analyzer, mock_logger)
    
    with pytest.raises(MissingDataError):
        pipeline.run(AnalysisMode.FULL, args=Mock())
    
    provider.load_core_events.assert_not_called()