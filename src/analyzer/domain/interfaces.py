from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Set, Tuple, Union
import pandas as pd
import polars as pl

from ..domain.types import (
    EventStream,
    RepositoryCreationTable,
    StratifiedRepositoriesDataset
)

ProcessModelArtifact = Any
EventLogArtifact = Any

@dataclass
class ProcessAggregates:
    dfg: Dict[Tuple[str, str], int] = field(default_factory=dict)
    activity_counts: Dict[str, int] = field(default_factory=dict)
    start_activities: Dict[str, int] = field(default_factory=dict)
    end_activities: Dict[str, int] = field(default_factory=dict)
    performance_matrix: Dict[Tuple[str, str], float] = field(default_factory=dict)


class IDataProvider(ABC):
    @abstractmethod
    def build_aggregates_lazyframe(self, archetype_repo_list: list[int]) -> pl.LazyFrame:
        pass

    @abstractmethod
    def load_core_events(self) -> EventStream:
        pass

    @abstractmethod
    def load_raw_repo_creation_events(self) -> RepositoryCreationTable:
        pass

    @abstractmethod
    def load_stratified_repositories(self) -> StratifiedRepositoriesDataset:
        pass

    @abstractmethod
    def load_single_archetype_model(
        self, 
        archetype_name: str, 
        model_type: str = "frequency"
    ) -> Optional[ProcessModelArtifact]:
        pass


class IResultWriter(ABC):
    @abstractmethod
    def write_metrics_parquet(self, lf: pl.LazyFrame, filename: str):
        pass

    @abstractmethod
    def write_dataframe(self, df: pl.DataFrame, filename: str):
        pass

    @abstractmethod
    def write_stratification_thresholds_json(self, thresholds: Dict[str, Any], filename: str):
        pass

    @abstractmethod
    def write_dataframe_final_analysis(self, df: pl.DataFrame, filename: str) -> None:
        pass

    @abstractmethod
    def write_heatmap(self, distance_matrix: Any, title: str, filename: str) -> None:
        pass

    @abstractmethod
    def write_difference_matrix_heatmap(self, diff_matrix: Any, title: str, filename: str, metric: str) -> None:
        pass

    @abstractmethod
    def save_event_log(self, event_log: EventLogArtifact, archetype_name: str):
        pass

    @abstractmethod
    def save_model_as_pickle(self, model: ProcessModelArtifact, archetype_name: str, model_type: str):
        pass

    @abstractmethod
    def save_model_visualization(self, model: ProcessModelArtifact, archetype_name: str, model_type: str):
        pass

    @abstractmethod
    def write_archetype_artifact_dataframe(
        self, 
        df: Any, 
        archetype_name: str, 
        filename: str
    ) -> None:
        pass

    @abstractmethod
    def save_identikit_image(self, df: pl.DataFrame, filename: str, title: str = "") -> None:
        pass

    @abstractmethod
    def save_radar_chart(self, df: pd.DataFrame, filename: str, title: str):
        pass
    
    @abstractmethod
    def save_dendrogram(self, distance_matrix: pd.DataFrame, filename: str, title: str):
        pass
    
    @abstractmethod
    def save_activity_grouped_bar_chart(self, data: pd.DataFrame, filename: str, title: str, top_n: int = 10):
        pass

    @abstractmethod
    def save_archetype_heatmap(self, matrix: pd.DataFrame, archetype_name: str, filename: str, title: str, metric: str = "frequency"):
        pass

    @abstractmethod
    def save_mds_plot(self, distance_matrix: pd.DataFrame, filename: str, title: str):
        pass

class IProcessAnalyzer(ABC):
    @abstractmethod
    def prepare_log(self, raw_ldf: pl.LazyFrame) -> EventLogArtifact:
        pass

    @abstractmethod
    def discover_heuristic_model_frequency(self, event_log: EventLogArtifact) -> ProcessModelArtifact:
        pass

    @abstractmethod
    def discover_heuristic_model_performance(self, event_log: EventLogArtifact) -> ProcessModelArtifact:
        pass

class IModelAnalyzer(ABC):
    @abstractmethod
    def get_adjacency_matrix_frequency(self, model: ProcessModelArtifact) -> Any:
        pass

    @abstractmethod
    def get_adjacency_matrix_performance(self, model: ProcessModelArtifact) -> Any:
        pass

    @abstractmethod
    def calculate_frobenius_distance(self, matrix1: Any, matrix2: Any, normalize: bool = True) -> float:
        pass
    
    @abstractmethod
    def get_model_nodes_set(self, model: ProcessModelArtifact) -> Set[str]:
        pass

    @abstractmethod
    def get_model_edges_set(self, model: ProcessModelArtifact) -> Set[Tuple[str, str]]:
        pass
        
    @abstractmethod
    def calculate_comparison_matrix(
            self, 
            matrix1: Any, 
            name1: str, 
            matrix2: Any, 
            name2: str,
            metric: str, 
            normalize: bool = False
        ) -> Any:
        pass
    
    @abstractmethod
    def get_process_complexity_metrics(self, model: ProcessModelArtifact) -> Dict[str, float]:
        pass

    @abstractmethod
    def calculate_jaccard_similarity(self, set_a: Set[Any], set_b: Set[Any]) -> float:
        pass

    @abstractmethod
    def get_nodes_count(self, model: ProcessModelArtifact) -> int:
        pass
    
    @abstractmethod
    def get_edges_count(self, model: ProcessModelArtifact) -> int:
        pass

    @abstractmethod
    def get_most_frequent_activity(self, model: ProcessModelArtifact) -> Tuple[str, int]:
        pass

    @abstractmethod
    def get_slowest_edge(self, model: ProcessModelArtifact) -> Tuple[Tuple[str, str], float]:
        pass