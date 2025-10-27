# File: tests/unit/test_pm4py_analyzer.py
import polars as pl
from datetime import datetime, timezone
from pm4py.objects.log.obj import EventLog
from pm4py.objects.heuristics_net.obj import HeuristicsNet

from src.dataset_analysis.infrastructure.pm4py_analyzer import PM4PyAnalyzer
from src.dataset_analysis.config import AnalysisConfig

def test_analyze_repo_process_normalization_and_discovery():
    """
    Verifica che PM4PyAnalyzer:
    1. Filtri correttamente gli eventi non-core.
    2. Normalizzi correttamente i nomi degli eventi (es. PR merged).
    3. Produca gli artefatti corretti (modelli e log).
    """
    # 1. Setup: Crea un DataFrame di input con eventi core e non-core
    input_df = pl.DataFrame({
        "activity": ["IssuesEvent", "PullRequestEvent", "WatchEvent", "PullRequestEvent"],
        "action": ["opened", "opened", "started", "closed"],
        "pr_merged": [None, None, None, True], # Questo deve diventare "merged"
        "actor_id": ["user1", "user1", "user2", "user1"],
        "timestamp": [
            datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 1, 11, 30, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ],
        # Aggiungi colonne opzionali richieste dalla funzione di normalizzazione
        "review_state": [None] * 4, "create_ref_type": [None] * 4, "delete_ref_type": [None] * 4,
    })

    analyzer = PM4PyAnalyzer(config=AnalysisConfig(dataset_directory="", output_directory="", start_date="", end_date=""))

    # 2. Azione
    results = analyzer.analyze_repository(input_df.lazy(), "test_repo_123")

    # 3. Assert
    assert results is not None
    assert "frequency_model" in results
    assert "performance_model" in results
    assert "filtered_log" in results

    # Verifica i tipi degli artefatti
    assert isinstance(results["frequency_model"], HeuristicsNet)
    assert isinstance(results["filtered_log"], EventLog)

    # Verifica la corretta normalizzazione e filtraggio degli eventi
    log = results["filtered_log"]
    assert len(log) == 1  # Il WatchEvent di user2 viene filtrato, quindi rimane solo user1
    assert len(log[0]) == 3  # 3 eventi per user1

    event_names = [event["concept:name"] for event in log[0]]
    expected_names = ["IssuesEvent_opened", "PullRequestEvent_opened", "PullRequestEvent_merged"]
    assert event_names == expected_names