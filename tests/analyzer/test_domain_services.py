import pytest
import polars as pl
from datetime import datetime, timezone
import numpy as np

from src.analyzer.domain import services as domain_services
from src.analyzer.domain.errors import DomainContractError, CalculationError
from src.analyzer.domain import predicates 

@pytest.fixture
def dummy_metrics_lf():
    data = {
        "repo_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "workload_cum": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        "collaboration_intensity_cum": [0, 0, 0, 0, 2, 3, 4, 5, 6, 7], 
        "community_engagement_cum": [5, 5, 5, 5, 5, 5, 5, 5, 5, 5],
        "external_popularity_cum": [0, 0, 0, 0, 0, 0, 0, 0, 0, 10], 
        "workload_norm": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        "collaboration_intensity_norm": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        "community_engagement_norm": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        "external_popularity_norm": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        "age_in_days": [10] * 10
    }
    return pl.DataFrame(data).lazy()

def test_build_lookup_table_success():
    raw_events = pl.DataFrame({
        "repo_id": [1, 1, 2, 3, 2],
        "timestamp": [datetime(2023, 1, 1), datetime(2023, 1, 5), datetime(2023, 1, 2), datetime(2023, 1, 3), datetime(2023, 1, 4)]
    })
    
    lookup = domain_services.extract_analyzable_repository(raw_events)
    
    assert lookup.shape == (3, 2)
    assert lookup.filter(pl.col("repo_id") == 1)["repo_creation_date"][0].date() == datetime(2023, 1, 1).date()
    assert "repo_creation_date" in lookup.columns

def test_build_lookup_table_empty():
    raw_events = pl.DataFrame(schema={"repo_id": pl.Int64, "timestamp": pl.Datetime})
    lookup = domain_services.extract_analyzable_repository(raw_events)
    assert lookup.is_empty()

def test_build_lookup_table_missing_columns():
    raw_events = pl.DataFrame({"repo_id": [1]})
    with pytest.raises(DomainContractError):
        domain_services.extract_analyzable_repository(raw_events)

def test_compute_thresholds_success(dummy_metrics_lf):
    quantiles = [0.5, 0.9]
        
    thresholds = domain_services.compute_quantiles_for_metrics(dummy_metrics_lf, quantiles)

    assert thresholds["workload_cum"]["Q50"] == pytest.approx(55.0)
    assert thresholds["collaboration_intensity_cum"]["Q50"] == pytest.approx(4.5) 
    assert thresholds["collaboration_intensity_norm"]["Q50"] == pytest.approx(0.0)

def test_compute_thresholds_empty_metrics():
    empty_lf = pl.DataFrame(schema={
        "workload_cum": pl.Float64, "collaboration_intensity_cum": pl.Float64,
        "community_engagement_cum": pl.Float64, "external_popularity_cum": pl.Float64,
        "workload_norm": pl.Float64, "collaboration_intensity_norm": pl.Float64,
        "community_engagement_norm": pl.Float64, "external_popularity_norm": pl.Float64,
    }).lazy()

    with pytest.raises(CalculationError):
        domain_services.compute_quantiles_for_metrics(empty_lf, [0.5])

@pytest.fixture
def core_events_lf():
    events = {
        "repo_id": [10, 10, 10, 20, 20, 30],
        "activity": ["CreateEvent", "PushEvent", "WatchEvent", "PushEvent", "ForkEvent", "PushEvent"],
        
        "timestamp": [
            datetime(2024, 1, 1, tzinfo=timezone.utc), 
            datetime(2024, 1, 5, tzinfo=timezone.utc), 
            datetime(2024, 1, 5, tzinfo=timezone.utc), 
            datetime(2024, 1, 2, tzinfo=timezone.utc), 
            datetime(2024, 1, 6, tzinfo=timezone.utc), 
            datetime(2024, 1, 15, tzinfo=timezone.utc)
        ],
        "actor_login": ["user_a", "user_a", "user_b", "user_c", "user_c", "user_d"],
        "actor_id": [1, 1, 2, 3, 3, 4],
        "push_size": [0, 10, 0, 5, 0, 15],
    }
    
    df = pl.DataFrame(events).with_columns(
        pl.col("timestamp").cast(pl.Datetime(time_unit='us', time_zone='UTC'))
    )
    return df.lazy()

def test_build_summary_metrics_age_and_norm(core_events_lf):
    lookup = pl.DataFrame({
        "repo_id": [10, 20, 30],
        
        "repo_creation_date": [datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 1, 2, tzinfo=timezone.utc), datetime(2024, 1, 10, tzinfo=timezone.utc)]
    })
    
    end_date = "2024-01-11T00:00:00Z" 
    
    metrics_lf = domain_services.calculate_metrics_for_repository(
        core_events_lf.with_columns(pl.lit(None).alias("action")), 
        lookup, predicates.is_significant_pop_event, 
        predicates.is_significant_eng_event, predicates.is_significant_collab_event,
        analysis_end_date=end_date
    )
    
    metrics_df = metrics_lf.collect()
    
    repo_10_row = metrics_df.filter(pl.col("repo_id") == 10)
    assert repo_10_row["age_in_days"][0] == 10
    assert repo_10_row["workload_cum"][0] == 10
    assert repo_10_row["workload_norm"][0] == pytest.approx(1.0)

    repo_30_row = metrics_df.filter(pl.col("repo_id") == 30)
    assert repo_30_row["age_in_days"][0] == 1
    assert repo_30_row["workload_cum"][0] == 15
    assert repo_30_row["workload_norm"][0] == pytest.approx(15.0 / 1.0) 

def test_build_summary_metrics_collaboration(core_events_lf):
    lookup = pl.DataFrame({"repo_id": [10, 20, 30], "repo_creation_date": [datetime(2024, 1, 1, tzinfo=timezone.utc)] * 3})
    end_date = "2024-01-31T00:00:00Z"

    metrics_lf = domain_services.calculate_metrics_for_repository(
        
        core_events_lf.with_columns(pl.lit(None).alias("action")),
        lookup, predicates.is_significant_pop_event,
        predicates.is_significant_eng_event,
        (pl.col("activity") == "PushEvent"),
        analysis_end_date=end_date
    )

    metrics_df = metrics_lf.collect()
       
    assert metrics_df.filter(pl.col("repo_id") == 10)["collaboration_intensity_cum"][0] == 1
    assert metrics_df.filter(pl.col("repo_id") == 20)["collaboration_intensity_cum"][0] == 1