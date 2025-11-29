
import pytest
import polars as pl
from datetime import datetime
from src.analyzer.infrastructure.data_provider import ParquetDataProvider
from src.analyzer.config import AnalysisConfig
from unittest.mock import Mock

@pytest.fixture
def provider_config(tmp_path, test_parquet_dataset):
    return AnalysisConfig(
        dataset_directory=test_parquet_dataset,
        output_directory=str(tmp_path / "output"),
        start_date="2023-01-01T00:00:00Z",
        end_date="2023-01-03T23:59:59Z" 
    )

def test_provider_load_core_events_lazy_scan(provider_config):
    provider = ParquetDataProvider(
        dataset_directory=provider_config.dataset_directory,
        start_date=provider_config.start_date,
        end_date=provider_config.end_date,
        analyzable_repositories_file=provider_config.analyzable_repositories_file,
        stratified_repositories_file=provider_config.stratified_repositories_parquet,
        output_directory=provider_config.output_directory,
        aggregate_model_subdirectory_name=provider_config.archetype_models_subdirectory_name, 
        archetype_process_models_directory=provider_config.archetype_process_models_directory
    )
    
    lf = provider.load_core_events()
    df = lf.collect() 
    
    assert df.shape[0] == 5
    assert "repo_id" in df.columns
    
def test_provider_load_repo_creation_events(provider_config):
    provider = ParquetDataProvider(
        dataset_directory=provider_config.dataset_directory,
        start_date=provider_config.start_date,
        end_date=provider_config.end_date,
        analyzable_repositories_file=provider_config.analyzable_repositories_file,
        stratified_repositories_file=provider_config.stratified_repositories_parquet,
        output_directory=provider_config.output_directory,
        aggregate_model_subdirectory_name=provider_config.archetype_models_subdirectory_name, 
        archetype_process_models_directory=provider_config.archetype_process_models_directory
    )

    df_creation = provider.load_raw_repo_creation_events()
    
    assert df_creation.shape[0] == 1
    assert df_creation["repo_id"][0] == 101 
    assert "timestamp" in df_creation.columns