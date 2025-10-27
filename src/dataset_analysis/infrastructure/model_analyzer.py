import os
import pickle
import logging
from typing import Optional
from pm4py.objects.heuristics_net.obj import HeuristicsNet

from ..domain.interfaces import IModelAnalyzer
from ..application.errors import DataPreparationError
from ..infrastructure.logging_config import LayerLoggerAdapter


class PM4PyModelAnalyzer(IModelAnalyzer):
    """
    Adapter del layer Infrastructure che implementa la porta di uscita IModelAnalyzer.
    Integra la libreria PM4Py per ispezionare oggetti HeuristicsNet, calcolare KPI
    strutturali e di performance, e restituire metriche di processo derivate.
    """

    def __init__(self):
        """Inizializza il sistema di logging dedicato all'analyzer."""
        base_logger = logging.getLogger(self.__class__.__name__)
        self.logger = LayerLoggerAdapter(base_logger, {"layer": "Infrastructure"})

    # =====================================================================
    # CARICAMENTO MODELLO
    # =====================================================================

    def load_model_from_file(self, file_path: str) -> Optional[HeuristicsNet]:
        """
        Carica un modello di processo (HeuristicsNet) da file pickle.
        Solleva un DataPreparationError se il file non è valido o non contiene
        un oggetto di tipo HeuristicsNet.
        """
        try:
            if not os.path.exists(file_path):
                self.logger.warning(f"File modello non trovato al percorso: {file_path}")
                return None

            with open(file_path, "rb") as f:
                model = pickle.load(f)

            if not isinstance(model, HeuristicsNet):
                self.logger.warning(f"Il file {file_path} non contiene un oggetto HeuristicsNet valido.")
                return None

            self.logger.info(f"Modello HeuristicsNet caricato correttamente da {file_path}.")
            return model

        except (FileNotFoundError, pickle.UnpicklingError, EOFError) as e:
            self.logger.error(f"Impossibile caricare il modello da {file_path}: {e}")
            return None # Restituisce None, non solleva un'eccezione
        except Exception as e:
            self.logger.error(f"Errore imprevisto durante il caricamento del modello da {file_path}: {e}", exc_info=True)
            return None

    # =====================================================================
    # KPI STRUTTURALI
    # =====================================================================

    def count_activities(self, model: HeuristicsNet) -> int:
        """KPI Aggiuntivo: Conta il numero di nodi (attività) presenti nel modello."""
        nodes_map = getattr(model, "nodes", {}) or {}
        return len(nodes_map)

    def count_connections(self, model: HeuristicsNet) -> int:
        """KPI Aggiuntivo: Conta il numero complessivo di archi diretti nel grafo."""
        nodes_map = getattr(model, "nodes", {}) or {}
        if not nodes_map:
            return 0
        return sum(len(getattr(node, "output_connections", {})) for node in nodes_map.values())

    # =====================================================================
    # KPI PRINCIPALI — Semantica consolidata (8 indicatori)
    # =====================================================================

    def is_issue_driven(self, model: HeuristicsNet) -> bool:
        """
        [KPI #1] Indice di Ascolto Attivo.
        Verifica se esiste un arco diretto da un'apertura di Issue a un'attività
        di inizio sviluppo (branch o pull request).
        """
        nodes_map = getattr(model, "nodes", {}) or {}
        start_node = nodes_map.get("IssuesEvent_opened")
        if not start_node:
            return False

        development_activities = {"CreateEvent_branch", "PullRequestEvent_opened"}
        for target_node in getattr(start_node, "output_connections", {}):
            target_name = getattr(target_node, "node_name", str(target_node))
            if target_name in development_activities:
                return True
        return False

    def get_issue_reaction_time_hours(self, perf_model: HeuristicsNet) -> Optional[float]:
        """
        [KPI #2] Latenza di Reazione Diretta.
        Tempo medio sull’arco diretto 'IssuesEvent_opened' → 'PullRequestEvent_opened'.
        """
        return self._get_direct_performance_between(
            perf_model, "IssuesEvent_opened", "PullRequestEvent_opened"
        )

    def has_review_loop(self, model: HeuristicsNet) -> bool:
        """
        [KPI #4] Presenza di Dibattito Tecnico.
        Determina se nel grafo esiste un ciclo di revisione: feedback → correzione → feedback.
        """
        nodes_map = getattr(model, "nodes", {}) or {}
        if not nodes_map:
            return False

        review_feedback_activities = {
            "PullRequestReviewCommentEvent_created",
            "PullRequestReviewEvent_changes_requested",
        }
        correction_activities = {"PullRequestEvent_synchronize", "PushEvent"}

        for src_name, src_node in nodes_map.items():
            if src_name in review_feedback_activities:
                for mid_node in getattr(src_node, "output_connections", {}):
                    mid_name = getattr(mid_node, "node_name", str(mid_node))
                    if mid_name in correction_activities:
                        for end_node in getattr(mid_node, "output_connections", {}):
                            end_name = getattr(end_node, "node_name", str(end_node))
                            if end_name in review_feedback_activities:
                                return True
        return False

    def get_rework_intensity(self, freq_model: HeuristicsNet) -> float:
        """
        [KPI #5] Intensità del Dibattito.
        Misura la forza di connessione 'commento → correzione' nella dependency_matrix.
        """
        dep_matrix = getattr(freq_model, "dependency_matrix", {}) or {}
        return float(
            dep_matrix.get(
                ("PullRequestReviewCommentEvent_created", "PullRequestEvent_synchronize"), 0.0
            )
        )

    def get_direct_merge_latency_hours(self, perf_model: HeuristicsNet) -> Optional[float]:
        """
        [KPI #6] Latenza di Merge Diretto.
        Tempo medio sull’arco 'PullRequestEvent_opened' → 'PullRequestEvent_merged'.
        """
        return self._get_direct_performance_between(
            perf_model, "PullRequestEvent_opened", "PullRequestEvent_merged"
        )

    def get_pr_success_rate(self, freq_model: HeuristicsNet) -> Optional[float]:
        """
        KPI Aggiuntivo: Percentuale di Pull Request unite con successo.
        """
        node_freq = getattr(freq_model, "node_frequencies", {}) or {}
        merged = node_freq.get("PullRequestEvent_merged", 0)
        rejected = node_freq.get("PullRequestEvent_rejected", 0)
        total = merged + rejected
        return merged / total if total > 0 else None

    # =====================================================================
    # METODI DI SUPPORTO
    # =====================================================================

    def _get_direct_performance_between(
        self, perf_model: HeuristicsNet, activity_start: str, activity_end: str
    ) -> Optional[float]:
        """
        Estrae il tempo medio (in ore) sull’arco diretto tra due attività specifiche.
        """
        perf_dfg = getattr(perf_model, "performance_dfg", {}) or {}
        if not perf_dfg:
            return None

        time_seconds = perf_dfg.get((activity_start, activity_end))
        if time_seconds and isinstance(time_seconds, (int, float)):
            return time_seconds / 3600.0
        return None
