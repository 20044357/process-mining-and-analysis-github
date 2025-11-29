from typing import Any, Optional, Dict, List
from dataclasses import dataclass
from enum import Enum


EventStream = Any
RepositoryCreationTable = Any
StratifiedRepositoriesDataset = Any


@dataclass
class AnalysisArtifacts:
    repo_id: str
    archetype_name: str
    freq_model_path: str
    perf_model_path: str