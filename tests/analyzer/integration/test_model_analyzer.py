
import pytest
from unittest.mock import Mock, MagicMock
import numpy as np
import pandas as pd
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from analyzer.domain.constants import ALL_POSSIBLE_ACTIVITIES
from src.analyzer.infrastructure.model_analyzer import PM4PyModelAnalyzer
from src.analyzer.domain.errors import CalculationError

ACT_A = "PushEvent"
ACT_B = "PullRequestEvent_opened"
ACT_C = "IssuesEvent_closed"

@pytest.fixture
def mock_heuristics_net_simple():
    model = MagicMock(spec=HeuristicsNet)
    
    
    model.dfg = {
        (ACT_A, ACT_B): 10,
        (ACT_B, ACT_C): 5,
        (ACT_B, ACT_A): 2, 
    }
    model.activities_occurrences = {ACT_A: 12, ACT_B: 17, ACT_C: 5}
    model.performance_dfg = {
        (ACT_A, ACT_B): 3600.0,
        (ACT_B, ACT_C): 1800.0, 
        (ACT_B, ACT_A): 10.0,
    }
    
    mock_node_b = Mock(output_connections=[
        Mock(node_name=ACT_C),
        Mock(node_name=ACT_A)
    ], input_connections=[Mock(), Mock()]) 
    
    mock_node_a = Mock(output_connections=[Mock(node_name=ACT_B)], input_connections=[Mock()]) 
    
    model.nodes = {
        ACT_A: mock_node_a,
        ACT_B: mock_node_b,
        ACT_C: Mock(output_connections=[], input_connections=[Mock()])
    }
    
    model.activities = [ACT_A, ACT_B, ACT_C] 
    
    return model

@pytest.fixture
def analyzer():
    return PM4PyModelAnalyzer()

def test_get_nodes_and_edges_count(analyzer, mock_heuristics_net_simple):
    assert analyzer.get_nodes_count(mock_heuristics_net_simple) == 3
    assert analyzer.get_edges_count(mock_heuristics_net_simple) == 3

def test_get_most_frequent_activity(analyzer, mock_heuristics_net_simple): 
    act, count = analyzer.get_most_frequent_activity(mock_heuristics_net_simple)
    assert act == ACT_B 
    assert count == 17

def test_get_slowest_edge(analyzer, mock_heuristics_net_simple):
    
    edge, time = analyzer.get_slowest_edge(mock_heuristics_net_simple)
    assert edge == (ACT_A, ACT_B) 
    assert time == 3600.0

def test_get_process_complexity_metrics(analyzer, mock_heuristics_net_simple):    
    metrics = analyzer.get_process_complexity_metrics(mock_heuristics_net_simple)

    assert metrics["cyclomatic_complexity"] == 1.0
    assert metrics["density"] == pytest.approx(0.5)

def test_get_adj_matrix_frequency(analyzer, mock_heuristics_net_simple):
    df_freq = analyzer.get_adjacency_matrix_frequency(mock_heuristics_net_simple)
    
    assert df_freq.shape == (len(ALL_POSSIBLE_ACTIVITIES), len(ALL_POSSIBLE_ACTIVITIES))
    assert df_freq.loc['PushEvent', 'PullRequestEvent_opened'] == 10.0
    assert df_freq.loc['PullRequestEvent_opened', 'PushEvent'] == 2.0    
    assert df_freq.loc['PushEvent', 'IssuesEvent_closed'] == 0.0

def test_calculate_jaccard_similarity():
    set_a = {'Push', 'Review', 'Issue'}
    set_b = {'Push', 'Review', 'Merge', 'Deploy'}
    
    analyzer = PM4PyModelAnalyzer()
    similarity = analyzer.calculate_jaccard_similarity(set_a, set_b)
    assert similarity == pytest.approx(0.4)

def test_calculate_frobenius_distance_normalized():
    m1 = pd.DataFrame([[10, 0], [0, 5]])
    m2 = pd.DataFrame([[5, 0], [0, 10]])

    analyzer = PM4PyModelAnalyzer()
    distance = analyzer.calculate_frobenius_distance(m1, m2, normalize=True)
    assert distance == pytest.approx(0.4714045) 

def test_calculate_comparison_matrix_normalized_frequency():
    m1 = pd.DataFrame([[10, 20], [5, 5]], index=['A', 'B'], columns=['A', 'B'])
    
    m2 = pd.DataFrame([[10, 0], [0, 20]], index=['A', 'B'], columns=['A', 'B']) 
    
    analyzer = PM4PyModelAnalyzer()
    diff_matrix = analyzer.calculate_comparison_matrix(m1, 'A', m2, 'B', metric='frequency', normalize=True)
    
    
    assert diff_matrix.loc['A', 'B'] == pytest.approx(0.5)
    assert diff_matrix.loc['B', 'B'] == pytest.approx(0.125 - 0.6666666666666666)

    
    
    assert diff_matrix.loc['A', 'A'] == pytest.approx(0.25 - 0.3333333333333333)