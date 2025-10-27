# File: tests/unit/test_utils.py
import polars as pl
from polars.testing import assert_frame_equal
from src.dataset_analysis.utils import scan_parquet_dataset_only, filter_core_workflow_events_lazy

def test_scan_parquet_dataset_only_generates_correct_paths(test_parquet_dataset, capsys):
    """Verifica che la funzione generi il numero corretto di pattern di path."""
    base_dir = test_parquet_dataset
    start_date = "2023-01-01T00:00:00Z"
    end_date = "2023-01-02T23:59:59Z"

    scan_parquet_dataset_only(base_dir, start_date, end_date)
    captured = capsys.readouterr()

    # Test robusto: controlla solo il messaggio di riepilogo che viene stampato.
    expected_summary_message = "[Utils] Generati 2 pattern di percorsi Parquet"
    assert expected_summary_message in captured.out

def test_filter_core_workflow_events_lazy():
    """Verifica la logica di filtraggio degli eventi core."""
    mock_data = pl.DataFrame({
        "activity": ["PushEvent", "IssuesEvent", "IssuesEvent", "PullRequestEvent", "WatchEvent", "CommitCommentEvent"],
        "action": [None, "opened", "closed", "synchronize", "started", "created"],
        "create_ref_type": [None, None, None, None, None, None]
    })
    
    lf = mock_data.lazy()
    filtered_lf = filter_core_workflow_events_lazy(lf)
    result_df = filtered_lf.collect()

    expected_df = pl.DataFrame({
        "activity": ["PushEvent", "IssuesEvent", "PullRequestEvent", "CommitCommentEvent"],
        "action": [None, "opened", "synchronize", "created"],
        "create_ref_type": [None, None, None, None]
    })
    
    assert len(result_df) == 4
    assert_frame_equal(result_df, expected_df, check_row_order=False)