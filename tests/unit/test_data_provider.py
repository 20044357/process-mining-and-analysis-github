# File: tests/unit/test_data_provider.py

import zoneinfo
import polars as pl
from polars.testing import assert_frame_equal
import pytest
from datetime import datetime, timezone

from src.dataset_analysis.infrastructure.data_provider import ParquetDataProvider
from src.dataset_analysis.config import AnalysisConfig

def test_get_repo_lookup_table_extracts_creation_events(test_parquet_dataset):
    """
    Verifica che get_repo_lookup_table() estragga correttamente le repository
    dai CreateEvent presenti nel dataset.
    """
    from src.dataset_analysis.infrastructure.data_provider import ParquetDataProvider
    from src.dataset_analysis.config import AnalysisConfig
    import polars as pl

    config = AnalysisConfig(
        dataset_directory=test_parquet_dataset,
        output_directory="/tmp",
        start_date="2023-01-01T00:00:00Z",
        end_date="2023-01-03T23:59:59Z",
    )

    provider = ParquetDataProvider(config)

    # 1️⃣ Esegui il metodo reale
    lookup_df = provider.load_raw_repository_creation_events()

    # 2️⃣ Verifica la struttura e i valori
    assert isinstance(lookup_df, pl.DataFrame)
    assert "repo_id" in lookup_df.columns
    assert "repo_creation_date" in lookup_df.columns

    # 3️⃣ Deve esserci una sola repo creata (id=101)
    assert lookup_df.shape == (1, 2)
    assert lookup_df["repo_id"][0] == 101

    expected_dt = datetime(2023, 1, 1, 10, 0, tzinfo=zoneinfo.ZoneInfo("UTC"))
    assert lookup_df["repo_creation_date"][0] == expected_dt

def test_calculate_summary_metrics(test_parquet_dataset):
    """
    Verifica che le metriche Gold Standard siano calcolate correttamente dal ParquetDataProvider
    utilizzando il dataset di test definito in conftest.py.
    """
    # 1. Setup: Crea la configurazione e il provider puntando al dataset di test
    config = AnalysisConfig(
        dataset_directory=test_parquet_dataset,
        output_directory="/tmp",  # Non rilevante per questo test
        start_date="2023-01-01T00:00:00Z",
        end_date="2023-01-03T23:59:59Z",
    )
    provider = ParquetDataProvider(config)
    
    # La tabella di lookup deve contenere la repo 101, che esiste nel dataset di test
    lookup_table = pl.DataFrame({
        "repo_id": [101], 
        "repo_creation_date": [datetime(2023, 1, 1, 10, 0, 0)]
    })

    # 2. Azione: Esegui il metodo da testare e materializza il risultato
    result_lf = provider.calculate_summary_metrics(lookup_table)
    assert result_lf is not None, "calculate_summary_metrics() ha restituito None"
    
    result_df = result_lf.collect()

    # 3. Assert: Calcola i valori attesi basandoti sui dati in conftest.py
    # - volume_lavoro_cum: Somma di push_size -> 5.0 + 3.0 = 8.0
    # - intensita_collaborativa_cum: Attori unici (20) in eventi di collaborazione -> 1
    # - engagement_community_cum: 1 'IssuesEvent' con action 'opened' -> 1
    # - popolarita_esterna_cum: 0 eventi (Watch, Fork, Release) -> 0
    # - age_in_days: L'analisi finisce il 2023-01-03. La repo è creata il 2023-01-01. Età ~ 3 giorni.
    #   (La data di fine analisi è al giorno 3, quindi l'età è 2.0)
    age = 2.0
    expected_df = pl.DataFrame({
        "repo_id": [101],
        "volume_lavoro_cum": [8.0],
        "popolarita_esterna_cum": [0.0],
        "engagement_community_cum": [1.0],
        "intensita_collaborativa_cum": [1.0],
        "repo_creation_date": [datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)],
        "age_in_days": [age],
        "volume_lavoro_norm": [8.0 / age],
        "intensita_collaborativa_norm": [1.0 / age],
        "popolarita_esterna_norm": [0.0 / age],
        "engagement_community_norm": [1.0 / age],
    # Forziamo i tipi di dato per matchare l'output di Polars
    }).with_columns(pl.col("repo_id").cast(pl.Int64))

    # Seleziona solo le colonne presenti nel risultato per un confronto robusto
    expected_df = expected_df.select(result_df.columns)

    assert_frame_equal(result_df, expected_df, check_row_order=False, check_dtypes=False)

def test_calculate_quantiles():
    """
    Verifica il calcolo dei quantili su un LazyFrame fittizio.
    Questo test è unitario e non dipende da file esterni.
    """
    # 1. Setup
    mock_metrics_data = pl.DataFrame({
        "volume_lavoro_cum": [float(i) for i in range(1, 101)],  # Valori da 1 a 100
        # Le altre metriche sono riempite con 0 per isolare il test
        "intensita_collaborativa_cum": [0.0] * 100, "engagement_community_cum": [0.0] * 100,
        "popolarita_esterna_cum": [0.0] * 100, "volume_lavoro_norm": [0.0] * 100,
        "intensita_collaborativa_norm": [0.0] * 100, "engagement_community_norm": [0.0] * 100,
        "popolarita_esterna_norm": [0.0] * 100,
    })
    mock_metrics_lf = mock_metrics_data.lazy()

    config = AnalysisConfig(
        dataset_directory="", output_directory="",
        start_date="", end_date="",
    )
    # Impostiamo i quantili direttamente nell'oggetto config per il test
    config.QUANTILES = [0.5, 0.9] 
    provider = ParquetDataProvider(config)

    # 2. Azione
    thresholds = provider.calculate_quantiles(mock_metrics_lf, config)

    # 3. Assert
    # Per una sequenza da 1 a 100, i quantili sono prevedibili
    assert "volume_lavoro_cum" in thresholds
    assert thresholds["volume_lavoro_cum"]["Q50"] == pytest.approx(50.5)
    assert thresholds["volume_lavoro_cum"]["Q90"] == pytest.approx(90.1)
    
    # Verifica che le metriche nulle abbiano quantili a zero
    assert thresholds["intensita_collaborativa_cum"]["Q50"] == 0.0