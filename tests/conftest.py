# File: tests/conftest.py
import pytest
import polars as pl
import os
from pathlib import Path
from datetime import datetime, timezone

@pytest.fixture(scope="session")
def test_parquet_dataset(tmp_path_factory):
    """
    Crea un mini dataset Parquet fittizio e robusto in una struttura temporanea.
    """
    base_dir = tmp_path_factory.mktemp("dataset_distillato")
    
    # Dati progettati per assicurare che repo_id=101 sopravviva ai filtri
    data = {
        # Repo 101: Creata nel periodo, ha push e attori, sopravviverà
        'activity': [
            "CreateEvent", "PushEvent", "PushEvent", "IssuesEvent", "PullRequestEvent"
        ],
        'timestamp': [
            datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 2, 9, 0, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 2, 10, 0, 0, tzinfo=timezone.utc)
        ],
        'actor_id': [10, 10, 20, 10, 20],
        'repo_id': [101, 101, 101, 101, 101],
        'actor_login': ["user_a", "user_a", "user_b", "user_a", "user_b"],
        'push_size': [None, 5.0, 3.0, None, None],
        'action': [None, None, None, "opened", "opened"],
        'create_ref_type': ["repository", None, None, None, None],
        'pr_merged': [None, None, None, None, None],
    }
    
    full_data = {**data}
    num_rows = len(data['activity'])
    all_cols = ['repo_name', 'payload_type', 'org_login', 'push_ref', 'push_head', 'push_commit_count', 
                'issue_number', 'issue_state', 'comment_id', 'issue_created_at', 'issue_closed_at', 
                'master_branch', 'delete_ref', 'delete_ref_type', 'pr_number', 'pr_additions', 
                'pr_deletions', 'pr_changed_files', 'pr_created_at', 'pr_merged_at', 'pr_closed_at', 
                'create_ref', 'review_id', 'review_state', 'fork_full_name', 'fork_id', 'forkee_stars', 
                'forkee_forks', 'release_id', 'release_tag_name', 'issue_labels', 'issue_label_count', 
                'member_id', 'member_login', 'gollum_page_count', 'comment_commit_id', 'comment_path']
    for col in all_cols:
        full_data[col] = [None] * num_rows

    df = pl.DataFrame(full_data).with_columns(
        pl.col("timestamp").cast(pl.Datetime(time_unit='ns', time_zone='UTC')),
        pl.col("repo_id").cast(pl.Int64),
        pl.col("actor_id").cast(pl.Int64)
    )

    # Scrivi il file nella struttura partizionata
    for row in df.iter_rows(named=True):
        ts = row['timestamp']
        partition_dir = base_dir / f"anno={ts.year}" / f"mese={ts.month:02d}" / f"giorno={ts.day:02d}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        row_df = df.filter(pl.col('timestamp') == ts)
        file_path = partition_dir / f"event_{ts.isoformat().replace(':', '-')}.parquet"
        row_df.write_parquet(file_path)

    return str(base_dir)

    # ------------------------------------------------------------------------------------------
    # Eventi generati per ogni repository
    # ------------------------------------------------------------------------------------------
    # repo_id = 101 → 5 eventi totali:
    #   ├─ 2023-01-01 10:00:00Z  → CreateEvent        (creazione repository)
    #   ├─ 2023-01-01 11:00:00Z  → PushEvent          (push_size = 5.0)
    #   ├─ 2023-01-01 12:00:00Z  → PushEvent          (push_size = 3.0)
    #   ├─ 2023-01-02 09:00:00Z  → IssuesEvent        (action = "opened")
    #   └─ 2023-01-02 10:00:00Z  → PullRequestEvent   (action = "opened")
    #
    #   Attori coinvolti:
    #       user_a (actor_id=10)
    #       user_b (actor_id=20)
    #
    #   Questi dati vengono usati nei test per verificare:
    #       - Somma push_size (8.0)
    #       - Numero attori collaborativi (1)
    #       - Numero issue aperte (1)
    #       - Assenza di eventi di popolarità (Watch/Fork/Release)
    #       - Calcolo età repo (3 giorni nel range temporale)
    # ------------------------------------------------------------------------------------------
