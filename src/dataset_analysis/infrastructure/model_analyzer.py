import os
import pickle
from typing import Optional, Any
import pm4py
from pm4py.objects.heuristics_net.obj import HeuristicsNet

from ..domain.interfaces import IModelAnalyzer

class PM4PyModelAnalyzer(IModelAnalyzer):
    """
    Implementazione di IModelAnalyzer che sa come ispezionare gli oggetti HeuristicsNet di PM4Py
    e calcolare KPI di processo sia strutturali che di performance.
    """

    def load_model_from_file(self, file_path: str) -> Optional[Any]:
        try:
            with open(file_path, 'rb') as f:
                return pickle.load(f)
        except (FileNotFoundError, pickle.UnpicklingError) as e:
            print(f"[ModelAnalyzer] ⚠️ Impossibile caricare il modello da {file_path}: {e}")
            return None

    # --- METODI ESISTENTI (KPI di Complessità e Collaborazione Base) ---

    def count_activities(self, model: HeuristicsNet) -> int:
        if not isinstance(model, HeuristicsNet) or not hasattr(model, 'nodes'):
            return 0
        return len(model.nodes)

    def count_connections(self, model: HeuristicsNet) -> int:
        if not isinstance(model, HeuristicsNet) or not hasattr(model, 'nodes') or not isinstance(model.nodes, dict):
            return 0
        total_connections = 0
        for node in model.nodes.values():
            if hasattr(node, 'output_connections'):
                total_connections += len(node.output_connections)
        return total_connections

    def has_review_loop(self, model: HeuristicsNet) -> bool:
        """Versione robusta che ispeziona la struttura finale del grafo."""
        if not isinstance(model, HeuristicsNet) or not hasattr(model, 'nodes') or not isinstance(model.nodes, dict):
            return False

        review_activities = {
            "IssueCommentEvent_created", "PullRequestReviewCommentEvent_created", 
            "PullRequestReviewEvent_changes_requested", "PullRequestReviewEvent_commented",
            "PullRequestReviewEvent_approved", "PullRequestReviewEvent_created"
        }
        correction_activities = {"PullRequestEvent_synchronize", "PushEvent"}

        try:
            node_to_name_map = {node: str(activity) for activity, node in model.nodes.items()}
        except Exception:
            return False

        for source_node, source_name in node_to_name_map.items():
            if source_name in review_activities:
                if not hasattr(source_node, 'output_connections'): continue
                for target_node in source_node.output_connections.keys():
                    target_name = node_to_name_map.get(target_node)
                    if target_name and target_name in correction_activities:
                        if not hasattr(target_node, 'output_connections'): continue
                        for final_target_node in target_node.output_connections.keys():
                            if final_target_node is source_node:
                                return True
        return False

    def get_review_cycle_performance(self, model: HeuristicsNet) -> Optional[float]:
        """Estrae il tempo mediano (in ore) del ciclo di review, se esiste."""
        if not isinstance(model, HeuristicsNet) or not hasattr(model, 'performance_dfg'):
            return None
        
        perf_dfg = model.performance_dfg
        if not perf_dfg: return None
            
        review_activities = {
            "IssueCommentEvent_created", "PullRequestReviewCommentEvent_created",
            "PullRequestReviewEvent_changes_requested", "PullRequestReviewEvent_commented",
            "PullRequestReviewEvent_approved", "PullRequestReviewEvent_created"
        }
        correction_activities = {"PullRequestEvent_synchronize", "PushEvent"}

        cycle_times_seconds = []
        for (source_obj, target_obj), perf_value in perf_dfg.items():
            source_name, target_name = str(source_obj), str(target_obj)
            if source_name in review_activities and target_name in correction_activities:
                if isinstance(perf_value, (int, float)):
                    cycle_times_seconds.append(perf_value)
        
        if cycle_times_seconds:
            return (sum(cycle_times_seconds) / len(cycle_times_seconds)) / 3600.0
        return None

    # --- NUOVI METODI (KPI Avanzati per l'Analisi di Crescita) ---

    def is_issue_driven(self, model: HeuristicsNet) -> bool:
        """
        Verifica se esiste una connessione diretta da 'IssuesEvent_opened' a un'attività di sviluppo.
        Questo KPI misura il "Grado di Attenzione alla Community".
        """
        if not isinstance(model, HeuristicsNet) or not hasattr(model, 'nodes') or not isinstance(model.nodes, dict):
            return False

        try:
            node_to_name_map = {node: str(activity) for activity, node in model.nodes.items()}
            name_to_node_map = {str(activity): node for activity, node in model.nodes.items()}
        except Exception:
            return False

        start_node = name_to_node_map.get("IssuesEvent_opened")
        if not start_node or not hasattr(start_node, 'output_connections'):
            return False

        development_activities = {"CreateEvent_branch", "PullRequestEvent_opened"}

        for target_node in start_node.output_connections.keys():
            target_name = node_to_name_map.get(target_node)
            if target_name and target_name in development_activities:
                return True
        
        return False

    def _get_performance_between(self, model: HeuristicsNet, activity_start: str, activity_end: str) -> Optional[float]:
        """Helper generico per estrarre il tempo mediano (in ore) tra due attività."""
        if not isinstance(model, HeuristicsNet) or not hasattr(model, 'performance_dfg'):
            return None
        
        perf_dfg = model.performance_dfg
        if not perf_dfg: return None

        for (source_obj, target_obj), performance_value in perf_dfg.items():
            if str(source_obj) == activity_start and str(target_obj) == activity_end:
                if isinstance(performance_value, (int, float)):
                    return performance_value / 3600.0  # Converte secondi in ore
                return None
        return None

    def get_issue_reaction_time(self, model: HeuristicsNet) -> Optional[float]:
        """
        Calcola il tempo mediano da 'IssuesEvent_opened' a 'PullRequestEvent_opened'.
        Questo KPI misura il "Tempo di Reazione alla Community".
        """
        return self._get_performance_between(model, "IssuesEvent_opened", "PullRequestEvent_opened")

    def get_pr_lead_time(self, model: HeuristicsNet) -> Optional[float]:
        """
        Calcola il tempo mediano da 'PullRequestEvent_opened' a 'PullRequestEvent_merged'.
        Questo KPI misura il "Tempo di Integrazione del Codice".
        """
        return self._get_performance_between(model, "PullRequestEvent_opened", "PullRequestEvent_merged")