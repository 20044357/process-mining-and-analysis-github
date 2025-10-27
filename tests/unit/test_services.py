# File: tests/unit/test_services.py
import polars as pl
from src.dataset_analysis.domain import services as domain_services
from src.dataset_analysis.config import AnalysisConfig

def test_get_stratum_archetype_name_corrected():
    """Testa la classificazione degli archetipi per vari ID di strato."""
    # Test per "Stella Nascente Collaborativa"
    strato_id_stella = "Alto|Alto|Medio|Basso|Gigante|Alto|Medio|Basso"
    assert domain_services.get_stratum_archetype_name_corrected(strato_id_stella) == "Stella Nascente Collaborativa"

    # Test per "Produttore Solitario Prolifico"
    strato_id_solitario = "Alto|Zero|Zero|Basso|Gigante|Zero|Zero|Basso"
    assert domain_services.get_stratum_archetype_name_corrected(strato_id_solitario) == "Produttore Solitario Prolifico"
    
    # Test per "Progetto Consolidato" (Legacy)
    strato_id_legacy = "Gigante|Alto|Medio|Medio|Basso|Basso|Basso|Basso"
    assert domain_services.get_stratum_archetype_name_corrected(strato_id_legacy) == "Progetto Consolidato"

    # Test per "Progetto Fantasma / Inattivo"
    strato_id_fantasma = "Zero|Zero|Zero|Zero|Zero|Zero|Zero|Zero"
    assert domain_services.get_stratum_archetype_name_corrected(strato_id_fantasma) == "Progetto Fantasma / Inattivo"
    
    # Test per caso malformato
    assert domain_services.get_stratum_archetype_name_corrected("Corto|Corto") == "ID Strato Malformato"

def test_stratify_lazy_with_monkeypatch(monkeypatch):
    """
    Verifica la logica di stratificazione con monkeypatch per QUANTILE_LABELS.
    """
    # 1️⃣ Setup
    metrics_lf = pl.DataFrame({
        "repo_id": [1, 2, 3, 4, 5],
        "volume_lavoro_norm": [0, 5, 50, 95, 1000],
    }).lazy()

    thresholds = {"volume_lavoro_norm": {"Q50": 10.0, "Q90": 90.0, "Q99": 100.0}}

    config = AnalysisConfig("", "", "", "")
    monkeypatch.setattr(config, "QUANTILE_LABELS", ["Basso", "Medio", "Alto", "Gigante"])

    # Nessuna patch di METRICS_TO_STRATIFY necessaria
    result_lf = domain_services.build_stratification_plan(metrics_lf, thresholds, config)
    result_df = result_lf.collect()

    expected = ["Zero", "Basso", "Medio", "Alto", "Gigante"]
    assert result_df["volume_lavoro_norm_cat"].to_list() == expected
    assert result_df["strato_id"].to_list() == expected

def test_select_representative_repos():
    """
    Verifica la selezione di repo rappresentative basata su una metrica.
    """
    # 1. Setup: Crea un DataFrame stratificato fittizio e completo
    stratified_df = pl.DataFrame({
        "repo_id": ["A", "B", "C", "D"],
        "age_in_days": [10, 20, 30, 40], # Colonna richiesta dalla funzione
        "popolarita_esterna_norm_cat": ["Gigante", "Gigante", "Basso", "Gigante"],
        "intensita_collaborativa_norm_cat": ["Gigante", "Gigante", "Alto", "Basso"],
        "volume_lavoro_cum": [100, 500, 200, 300], # Metrica di campionamento
        # Aggiungi tutte le altre colonne _cat richieste dalla funzione
        "volume_lavoro_cum_cat": ["Basso", "Alto", "Medio", "Medio"],
        "intensita_collaborativa_cum_cat": ["Basso", "Alto", "Medio", "Medio"],
        "engagement_community_cum_cat": ["Basso", "Alto", "Medio", "Medio"],
        "popolarita_esterna_cum_cat": ["Basso", "Alto", "Medio", "Medio"],
        "volume_lavoro_norm_cat": ["Basso", "Alto", "Medio", "Medio"],
        "engagement_community_norm_cat": ["Basso", "Alto", "Medio", "Medio"],
    })

    archetype_profiles = {
        "Giganti Collaborativi": (
            (pl.col("popolarita_esterna_norm_cat") == "Gigante") &
            (pl.col("intensita_collaborativa_norm_cat") == "Gigante")
        )
    }
    
    config = AnalysisConfig(dataset_directory="", output_directory="", start_date="", end_date="")

    # 2. Azione
    sampled_repos = domain_services.select_representative_repos(
        stratified_df,
        config=config,
        archetype_profiles=archetype_profiles,
        sampling_metric="volume_lavoro_cum"
    )

    # 3. Assert
    assert "Giganti Collaborativi" in sampled_repos
    # Repo A e B corrispondono al profilo. La funzione raggruppa per *tutte* le colonne _cat.
    # A e B hanno valori _cat diversi, quindi sono in gruppi diversi.
    # La funzione selezionerà il top per ogni gruppo, quindi le prenderà entrambe.
    assert sorted(sampled_repos["Giganti Collaborativi"]) == sorted(["A", "B"])

def test_count_by_stratum_computes_correct_distribution():
    """
    Verifica che count_by_stratum (o compute_stratum_distribution)
    conti correttamente il numero di repo per ciascuno strato.
    """
    # Dataset di input con 6 repo appartenenti a 3 strati
    df = pl.DataFrame({"strato_id": ["A", "A", "B", "B", "B", "C"]})

    # Esegui la funzione reale
    result_df = domain_services.calculate_strata_distribution(df)

    # ✅ Verifica le colonne esatte
    assert result_df.columns == ["strato_id", "num_repo", "perc_tot"]

    # ✅ Verifica che i valori siano corretti
    expected_counts = {"A": 2, "B": 3, "C": 1}
    total = 6
    expected_perc = {k: round(v / total * 100.0, 2) for k, v in expected_counts.items()}

    # Converti risultato in dizionario
    counts = dict(zip(result_df["strato_id"], result_df["num_repo"]))
    percs = dict(zip(result_df["strato_id"], result_df["perc_tot"]))

    assert counts == expected_counts
    for k in expected_perc:
        assert percs[k] == expected_perc[k]
