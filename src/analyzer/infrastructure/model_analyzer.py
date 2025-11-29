import os
import pickle
import logging
from typing import Dict, Optional, Any, Set, Tuple
import numpy as np
import pandas as pd
from pm4py.objects.heuristics_net.obj import HeuristicsNet
from ..domain.interfaces import IModelAnalyzer
from ..infrastructure.logging_config import LayerLoggerAdapter
from ..domain.constants import ALL_POSSIBLE_ACTIVITIES

class PM4PyModelAnalyzer(IModelAnalyzer):
    def __init__(self) -> None:
        base_logger = logging.getLogger(self.__class__.__name__)
        self.logger = LayerLoggerAdapter(base_logger, {"layer": "Infrastructure"})

    def load_model_from_file(self, file_path: str) -> Optional[HeuristicsNet]:
        try:
            if not os.path.exists(file_path):
                self.logger.warning(f"File modello non trovato: {file_path}")
                return None
            with open(file_path, "rb") as f:
                model = pickle.load(f)
            if not isinstance(model, HeuristicsNet):
                self.logger.warning(f"Il file {file_path} non è un HeuristicsNet valido.")
                return None
            return model
        except Exception as e:
            self.logger.error(f"Impossibile caricare il modello da {file_path}: {e}", exc_info=True)
            return None
        
    def get_adjacency_matrix_frequency(self, model: HeuristicsNet) -> pd.DataFrame:
            skeleton = ALL_POSSIBLE_ACTIVITIES            
            adj_matrix = pd.DataFrame(0.0, index=skeleton, columns=skeleton)
            
            if not hasattr(model, "nodes"):
                return adj_matrix

            for node_name, node_obj in model.nodes.items():
                for connected_node in node_obj.output_connections:
                    target_name = connected_node.node_name
        
                    if node_name in skeleton and target_name in skeleton:     
                        count = model.dfg.get((node_name, target_name), 0)
                        adj_matrix.loc[node_name, target_name] = count
                        
            return adj_matrix

    def get_adjacency_matrix_performance(self, model: HeuristicsNet) -> pd.DataFrame:
            skeleton = ALL_POSSIBLE_ACTIVITIES
            
            perf_matrix = pd.DataFrame(-1.0, index=skeleton, columns=skeleton)

            if not hasattr(model, "nodes") or model.performance_dfg is None:
                return perf_matrix
               
            for node_name, node_obj in model.nodes.items():
                for connected_node in node_obj.output_connections:
                    target_name = connected_node.node_name
                    
                    
                    if node_name in skeleton and target_name in skeleton:   
                        val = model.performance_dfg.get((node_name, target_name))
                        
                        if val is not None:
                            perf_matrix.loc[node_name, target_name] = val
                            
            return perf_matrix
    
    def calculate_comparison_matrix(
        self, 
        matrix1: pd.DataFrame, 
        name1: str, 
        matrix2: pd.DataFrame, 
        name2: str,
        metric: str, 
        normalize: bool = False 
    ) -> pd.DataFrame:
        if metric == 'frequency':
            fill_value = 0.0
        elif metric == 'performance':
            fill_value = -1.0
        else:
            raise ValueError("La metrica deve essere 'frequency' o 'performance'")

        m1_calc = matrix1.replace(fill_value, 0.0).copy()
        m2_calc = matrix2.replace(fill_value, 0.0).copy()
        
        if normalize:
            self.logger.debug(f"Normalizzazione attiva per {metric} ({name1} vs {name2})")
            total1 = m1_calc.sum().sum()
            total2 = m2_calc.sum().sum()
            
            if total1 > 0: m1_calc = m1_calc / total1
            if total2 > 0: m2_calc = m2_calc / total2

        mask1 = (matrix1 != fill_value)
        mask2 = (matrix2 != fill_value)
        valid_comparison_mask = mask1 | mask2 
        diff_values = m1_calc - m2_calc
   
        difference_matrix = np.where(
            valid_comparison_mask,
            diff_values,
            np.nan
        )
        
        return pd.DataFrame(
            difference_matrix, 
            index=matrix1.index, 
            columns=matrix1.columns
        )

    def get_model_nodes_set(self, model: HeuristicsNet) -> Set[str]:
        if not model:
            return set()
        
        
        if hasattr(model, 'activities_occurrences') and model.activities_occurrences is not None:
            return set(model.activities_occurrences.keys())
        
        elif hasattr(model, 'activities') and model.activities is not None:
            return set(model.activities)
        
        return set()

    def get_model_edges_set(self, model: HeuristicsNet) -> Set[Tuple[str, str]]:
        if not model:
            return set()

        edges = set()
        
        if hasattr(model, "nodes"):
            for node_name, node_obj in model.nodes.items():
                for connected_node in node_obj.output_connections:
                    target_name = connected_node.node_name
                    edges.add((node_name, target_name))
                    
        elif hasattr(model, "dfg"):
             edges = set(model.dfg.keys())
             
        return edges

    def calculate_jaccard_similarity(self, set_a: Set[Any], set_b: Set[Any]) -> float:
        if not set_a and not set_b:
            return 1.0 
        
        intersection = len(set_a.intersection(set_b))
        union = len(set_a.union(set_b))
        
        return float(intersection) / float(union) if union > 0 else 0.0
    
    def calculate_frobenius_distance(self, matrix1: pd.DataFrame, matrix2: pd.DataFrame, normalize: bool = True) -> float:
        all_cols = sorted(list(set(matrix1.columns) | set(matrix2.columns)))
        all_idx = sorted(list(set(matrix1.index) | set(matrix2.index)))
        
        m1 = matrix1.reindex(index=all_idx, columns=all_cols, fill_value=0.0).to_numpy()
        m2 = matrix2.reindex(index=all_idx, columns=all_cols, fill_value=0.0).to_numpy()

        
        if normalize:
            
            sum1 = np.sum(m1)
            sum2 = np.sum(m2)
            if sum1 > 0: m1 = m1 / sum1
            if sum2 > 0: m2 = m2 / sum2

        
        diff = m1 - m2
        return float(np.linalg.norm(diff, 'fro'))

    def get_nodes_count(self, model: HeuristicsNet) -> int:
        if not model or not hasattr(model, 'nodes'):
            return 0
        return len(model.nodes)

    def get_edges_count(self, model: HeuristicsNet) -> int:
        if not model or not hasattr(model, 'nodes'):
            return 0
        
        count = 0
        for node_obj in model.nodes.values():
            count += len(node_obj.output_connections)
        return count

    def get_most_frequent_activity(self, model: HeuristicsNet) -> Tuple[str, int]:
        if not model or not hasattr(model, 'nodes'):
            return ("None", 0)
                
        occs = getattr(model, 'activities_occurrences', {})
        
        best_act = "None"
        max_count = -1

        for node_name in model.nodes.keys():
            count = occs.get(node_name, 0)
            if count > max_count:
                max_count = count
                best_act = node_name
                
        return (str(best_act), int(max_count))

    def get_slowest_edge(self, model: HeuristicsNet) -> Tuple[Tuple[str, str], float]:
        if not model or not hasattr(model, 'nodes'):
            return (("None", "None"), 0.0)

        perf_data = getattr(model, 'performance_dfg', {}) or getattr(model, 'performance_matrix', {})
        
        slowest_edge = ("None", "None")
        max_time = -1.0

        for source, node_obj in model.nodes.items():
            for conn in node_obj.output_connections:
                target = conn.node_name
                
                time_val = perf_data.get((source, target), 0.0)
                
                if time_val > max_time:
                    max_time = time_val
                    slowest_edge = (source, target)

        return ((str(slowest_edge[0]), str(slowest_edge[1])), float(max_time))

    def get_process_complexity_metrics(self, model: HeuristicsNet) -> Dict[str, float]:
        n_nodes = self.get_nodes_count(model)
        n_edges = self.get_edges_count(model)
        
        if n_nodes <= 1:
            return {"density": 0.0, "avg_degree": 0.0, "cyclomatic_complexity": 0.0}
        
        avg_degree = float(n_edges) / float(n_nodes)
        possible_edges = n_nodes * (n_nodes - 1)
        density = float(n_edges) / float(possible_edges) if possible_edges > 0 else 0.0
        cyclomatic = n_edges - n_nodes + 1
        
        return {
            "density": density,
            "avg_degree": avg_degree,
            "cyclomatic_complexity": float(cyclomatic)
        }