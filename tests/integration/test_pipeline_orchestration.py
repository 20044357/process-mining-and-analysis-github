# File: tests/integration/test_pipeline_orchestration.py
from unittest.mock import MagicMock
import polars as pl
from datetime import datetime
import pytest

from src.dataset_analysis.application.pipeline import AnalysisPipeline, AnalysisMode
from src.dataset_analysis.domain.interfaces import IDataProvider, IProcessAnalyzer, IModelAnalyzer, ILogAnalyzer
from src.dataset_analysis.config import AnalysisConfig
# Importa il vero FileResultWriter
from src.dataset_analysis.infrastructure.file_writer import FileResultWriter

@pytest.fixture
def temp_analysis_config(tmp_path):
    """Crea un oggetto Config che usa una cartella temporanea per l'output."""
    return AnalysisConfig(
        dataset_directory="/fake/data", # Rimane fake perché il provider è mockato
        output_directory=str(tmp_path), # Usa il percorso temporaneo del test
        start_date="2023-01-01T00:00:00Z",
        end_date="2023-01-31T23:59:59Z",
    )

def test_full_pipeline_orchestration_with_real_writer(temp_analysis_config, tmp_path):
    """
    Testa l'integrazione della pipeline usando un VERO FileResultWriter
    per verificare che il flusso di I/O funzioni.
    """
    # 1. Crea i mock per le dipendenze che NON sono sotto test (Provider e Analyzers)
    mock_provider = MagicMock(spec=IDataProvider)
    mock_analyzer = MagicMock(spec=IProcessAnalyzer)
    mock_model_analyzer = MagicMock(spec=IModelAnalyzer)
    mock_log_analyzer = MagicMock(spec=ILogAnalyzer)

    # Usa un VERO writer che scrive nella cartella temporanea del test
    real_writer = FileResultWriter(temp_analysis_config)
    
    # 2. Configura i valori di ritorno dei mock
    mock_lookup_table = pl.DataFrame({"repo_id": [101], "repo_creation_date": [datetime.now()]})
    
    # Crea un LazyFrame basato su dati reali ma semplici
    mock_metrics_df = pl.DataFrame({
        "repo_id": [101], 
        "volume_lavoro_cum": [100.0],
        "intensita_collaborativa_cum": [10.0],
        "engagement_community_cum": [5.0],
        "popolarita_esterna_cum": [2.0],
        "volume_lavoro_norm": [1.0],
        "intensita_collaborativa_norm": [0.1],
        "engagement_community_norm": [0.05],
        "popolarita_esterna_norm": [0.02]
    })
    mock_metrics_lf = mock_metrics_df.lazy()

    mock_thresholds = {
        "volume_lavoro_cum": {"Q50": 50.0, "Q90": 90.0, "Q99": 99.0},
        "intensita_collaborativa_cum": {"Q50": 5.0, "Q90": 9.0, "Q99": 15.0},
        # ... (aggiungi le altre metriche per completezza)
        "engagement_community_cum": {"Q50": 0, "Q90": 0, "Q99": 0},
        "popolarita_esterna_cum": {"Q50": 0, "Q90": 0, "Q99": 0},
        "volume_lavoro_norm": {"Q50": 0, "Q90": 0, "Q99": 0},
        "intensita_collaborativa_norm": {"Q50": 0, "Q90": 0, "Q99": 0},
        "engagement_community_norm": {"Q50": 0, "Q90": 0, "Q99": 0},
        "popolarita_esterna_norm": {"Q50": 0, "Q90": 0, "Q99": 0}
    }

    mock_provider.get_repo_lookup_table.return_value = mock_lookup_table
    mock_provider.calculate_summary_metrics.return_value = mock_metrics_lf
    mock_provider.calculate_quantiles.return_value = mock_thresholds

    # 3. Inizializza la pipeline con il writer reale e gli altri mock
    pipeline = AnalysisPipeline(
        mock_provider, 
        mock_analyzer, 
        real_writer, # Usa il writer reale
        temp_analysis_config, 
        mock_model_analyzer, 
        mock_log_analyzer
    )

    # 4. Esegui la modalità da testare
    pipeline.run(mode=AnalysisMode.FULL)
    
    # 5. Assert: verifica che i file siano stati CREATI
    output_dir = tmp_path
    assert (output_dir / "repo_metrics_stratified.parquet").exists()
    assert (output_dir / "strata_distribution.csv").exists()
    
    # Opzionale: carica il risultato e controlla il contenuto
    result_df = pl.read_parquet(output_dir / "repo_metrics_stratified.parquet")
    assert len(result_df) == 1
    assert "strato_id" in result_df.columns