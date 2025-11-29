import pytest
import polars as pl
import os
from pathlib import Path
from datetime import datetime, timezone

@pytest.fixture(scope="session")
def test_parquet_dataset(tmp_path_factory):
    """
    Crea un mini dataset Parquet partizionato (anno/mese/giorno)
    usando Polars. Utile per testare IDataProvider.
    """
    base_dir = tmp_path_factory.mktemp("dataset")
    
    
    data = {
        'activity': ["CreateEvent", "PushEvent", "PushEvent", "IssuesEvent", "PullRequestEvent"],
        'timestamp': [
            datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc), 
            datetime(2023, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 2, 9, 0, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 2, 10, 0, 0, tzinfo=timezone.utc)
        ],
        'actor_id': [10, 10, 20, 10, 20],
        'repo_id': [101, 101, 101, 101, 101],
        'repo_name': ["user/repo1"] * 5, 
        'actor_login': ["user_a"] * 5,
        'push_size': [0, 5, 3, 0, 0],
        'action': ['create'] + [None] * 4, 
        'payload_type': ["Create", "Push", "Push", "Issue", "PullRequest"],
        
        'create_ref_type': ['repository', None, None, None, None], 
        'delete_ref_type': [None] * 5,
        'review_state': [None] * 5,
        'pr_merged': [None] * 5 
        
    }
    
    df = pl.DataFrame(data).with_columns(
        pl.col("timestamp").cast(pl.Datetime(time_unit='us', time_zone='UTC')), 
        pl.col("repo_id").cast(pl.Int64),
        pl.col("actor_id").cast(pl.Int64),
        pl.col("push_size").cast(pl.Float64),
        pl.col("create_ref_type").cast(pl.Utf8),
        pl.col("delete_ref_type").cast(pl.Utf8),
        pl.col("review_state").cast(pl.Utf8),
        pl.col("pr_merged").cast(pl.Boolean).fill_null(False), 
    )
    for row in df.iter_rows(named=True):
        ts = row['timestamp']
        partition_dir = base_dir / f"anno={ts.year}" / f"mese={ts.month:02d}" / f"giorno={ts.day:02d}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        row_df = df.filter(pl.col('timestamp') == ts)
        file_path = partition_dir / f"event_{ts.strftime('%H-%M-%S')}.parquet"
        row_df.write_parquet(file_path)

    return str(base_dir)