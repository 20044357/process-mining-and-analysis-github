# File: tests/unit/test_analyzers.py
from unittest.mock import MagicMock
import pandas as pd
from datetime import datetime, timedelta, timezone
import pm4py

from src.dataset_analysis.infrastructure.model_analyzer import PM4PyModelAnalyzer
from src.dataset_analysis.infrastructure.log_analyzer import PM4PyLogAnalyzer

def test_is_issue_driven_finds_connection():
    """Verifica che `is_issue_driven` ritorni True quando la connessione esiste."""
    # 1. Crea un mock del modello HeuristicsNet
    mock_model = MagicMock()
    
    # 2. Simula la struttura interna del grafo
    mock_start_node = MagicMock()
    mock_target_node = MagicMock()
    # PM4Py usa __repr__ o un attributo per identificare i nodi, simuliamo entrambi
    mock_target_node.node_name = "CreateEvent_branch" 
    
    mock_start_node.output_connections = [mock_target_node]
    
    mock_model.nodes = {
        "IssuesEvent_opened": mock_start_node,
        "CreateEvent_branch": mock_target_node,
    }

    # 3. Esegui il test
    analyzer = PM4PyModelAnalyzer()
    assert analyzer.is_issue_driven(mock_model) is True

def test_calculate_median_time_between():
    """Verifica il calcolo della mediana dei tempi di ciclo dal log."""
    # 1. Crea un DataFrame pandas per generare un EventLog
    log_df = pd.DataFrame([
        # Traccia 1: ciclo di 1 ora (3600s)
        {"case:concept:name": "actor1", "concept:name": "start", "time:timestamp": datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)},
        {"case:concept:name": "actor1", "concept:name": "end", "time:timestamp": datetime(2023, 1, 1, 11, 0, 0, tzinfo=timezone.utc)},
        # Traccia 2: due cicli, uno di 30 min (1800s), uno di 2 ore (7200s)
        {"case:concept:name": "actor2", "concept:name": "start", "time:timestamp": datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)},
        {"case:concept:name": "actor2", "concept:name": "end", "time:timestamp": datetime(2023, 1, 1, 12, 30, 0, tzinfo=timezone.utc)},
        {"case:concept:name": "actor2", "concept:name": "start", "time:timestamp": datetime(2023, 1, 1, 13, 0, 0, tzinfo=timezone.utc)},
        {"case:concept:name": "actor2", "concept:name": "end", "time:timestamp": datetime(2023, 1, 1, 15, 0, 0, tzinfo=timezone.utc)},
    ])
    event_log = pm4py.convert_to_event_log(log_df)

    # 2. Esegui il test
    analyzer = PM4PyLogAnalyzer()
    # Durate: [3600, 1800, 7200]. Mediana: 3600
    median_seconds = analyzer._calculate_median_time_between(event_log, "start", "end")

    # 3. Assert
    assert median_seconds == 3600.0