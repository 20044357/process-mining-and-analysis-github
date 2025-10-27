"""
Modulo del Domain Layer dedicato alle operazioni di analisi e calcolo
delle metriche, soglie e campioni rappresentativi delle repository.

Tutte le funzioni in questo modulo sono **pure**: non eseguono alcuna
operazione di I/O (come stampa o logging) e non dipendono da configurazioni
o oggetti appartenenti a layer superiori.
"""

from datetime import datetime
import polars as pl
from typing import Dict, List

from .predicates import is_bot_actor
from .types import SamplingReport, SamplingInfo
from .errors import DomainContractError, CalculationError, DomainError


# =====================================================================
# FASE 1 — COSTRUZIONE LOOKUP TABLE REPOSITORY
# =====================================================================

def build_repository_creation_lookup(raw_events_df: pl.DataFrame) -> pl.DataFrame:
    """
    Costruisce una lookup table univoca che associa a ogni repository
    la propria data di creazione, a partire dagli eventi grezzi.
    """
    required = {"repo_id", "timestamp"}
    missing = required - set(raw_events_df.columns)

    if missing:
        raise DomainContractError(f"Colonne mancanti nel DataFrame: {missing}")

    if raw_events_df.is_empty():
        return pl.DataFrame(schema={"repo_id": pl.Utf8, "repo_creation_date": pl.Datetime})

    return (
        raw_events_df
        .rename({"timestamp": "repo_creation_date"})
        .unique(subset=["repo_id"], keep="first")
    )


# =====================================================================
# FASE 2 — COSTRUZIONE METRICHE DI RIEPILOGO
# =====================================================================

def build_summary_metrics_lazy_plan(
    base_events_lf: pl.LazyFrame,
    repo_creation_lookup_df: pl.DataFrame,
    popularity_predicate: pl.Expr,
    engagement_predicate: pl.Expr,
    collaboration_predicate: pl.Expr,
    analysis_end_date: str
) -> pl.LazyFrame:
    """
    Costruisce il piano di esecuzione (lazy) per il calcolo delle metriche cumulative
    e normalizzate relative all'attività delle repository.
    """
    required_base = {"repo_id", "activity", "actor_login", "push_size", "actor_id"}
    schema = base_events_lf.collect_schema()
    missing_base = required_base - set(schema.names())
    if missing_base:
        raise DomainContractError(f"LazyFrame base non conforme: mancano {missing_base}")

    if {"repo_id", "repo_creation_date"} - set(repo_creation_lookup_df.columns):
        raise DomainContractError("Lookup table non conforme: colonne mancanti.")

    try:
        analysis_end_dt = datetime.fromisoformat(analysis_end_date.replace("Z", "+00:00"))
    except ValueError:
        raise DomainContractError(
            f"La data di fine analisi '{analysis_end_date}' non è in formato ISO 8601 valido."
        )

    repo_creation_lookup_lf = repo_creation_lookup_df.lazy()

    relevant_events_lf = (
        base_events_lf
        .join(repo_creation_lookup_lf, on="repo_id", how="semi")
        .filter(~is_bot_actor)
    )

    cumulative_sums_lf = (
        relevant_events_lf.with_columns([
            pl.when(pl.col("activity") == "PushEvent")
              .then(pl.col("push_size")).otherwise(0).alias("push_volume"),
            pl.when(popularity_predicate).then(1).otherwise(0).alias("popularity_event_count"),
            pl.when(engagement_predicate).then(1).otherwise(0).alias("engagement_event_count"),
        ])
        .group_by("repo_id")
        .agg([
            pl.sum("push_volume").alias("volume_lavoro_cum"),
            pl.sum("popularity_event_count").alias("popolarita_esterna_cum"),
            pl.sum("engagement_event_count").alias("engagement_community_cum"),
        ])
    )

    collaborator_counts_lf = (
        relevant_events_lf.filter(collaboration_predicate)
        .group_by("repo_id")
        .agg(pl.col("actor_id").n_unique().alias("intensita_collaborativa_cum"))
    )

    final_metrics_lf = (
        cumulative_sums_lf
        .join(collaborator_counts_lf, on="repo_id", how="left")
        .join(repo_creation_lookup_lf, on="repo_id", how="inner")
        .with_columns(
            pl.col("intensita_collaborativa_cum").fill_null(0),
            pl.col("repo_creation_date").cast(pl.Datetime(time_zone="UTC")),
            (pl.lit(analysis_end_dt) - pl.col("repo_creation_date"))
                .dt.total_days().alias("age_in_days")
        )
        .with_columns([
            (pl.col("volume_lavoro_cum") / pl.col("age_in_days")).fill_null(0).alias("volume_lavoro_norm"),
            (pl.col("intensita_collaborativa_cum") / pl.col("age_in_days")).fill_null(0).alias("intensita_collaborativa_norm"),
            (pl.col("popolarita_esterna_cum") / pl.col("age_in_days")).fill_null(0).alias("popolarita_esterna_norm"),
            (pl.col("engagement_community_cum") / pl.col("age_in_days")).fill_null(0).alias("engagement_community_norm"),
        ])
        .filter(pl.col("age_in_days") > 0)
    )

    return final_metrics_lf


# =====================================================================
# FASE 3 — CALCOLO SOGLIE DI STRATIFICAZIONE
# =====================================================================

def compute_stratification_thresholds(
    metrics_lf: pl.LazyFrame,
    quantiles_to_compute: List[float]
) -> Dict[str, Dict[str, float]]:
    """Calcola le soglie di stratificazione (quantili) per ogni metrica."""
    METRICS = [
        "volume_lavoro_cum", "intensita_collaborativa_cum",
        "engagement_community_cum", "popolarita_esterna_cum",
        "volume_lavoro_norm", "intensita_collaborativa_norm",
        "engagement_community_norm", "popolarita_esterna_norm",
    ]

    try:
        quantile_exprs = [
            pl.col(col).filter(pl.col(col) > 0)
            .quantile(q, interpolation="linear")
            .alias(f"{col}_Q{int(q * 100)}")
            for col in METRICS for q in quantiles_to_compute
        ]

        result_df = metrics_lf.select(quantile_exprs).collect(engine="streaming")
        result_dict = result_df.row(0, named=True)

        thresholds = {
            col: {
                f"Q{int(q * 100)}": float(result_dict.get(f"{col}_Q{int(q * 100)}", 0.0))
                for q in quantiles_to_compute
            }
            for col in METRICS
        }

        if not thresholds:
            raise CalculationError("Risultato vuoto nel calcolo delle soglie di stratificazione.")

        return thresholds

    except Exception as e:
        raise CalculationError(f"Errore nel calcolo delle soglie: {e}") from e


# =====================================================================
# FASE 4 — COSTRUZIONE DEL PIANO DI STRATIFICAZIONE
# =====================================================================

def build_stratification_plan(
    metrics_lf: pl.LazyFrame,
    thresholds: Dict[str, Dict[str, float]],
    quantile_labels: List[str]
) -> pl.LazyFrame:
    """Applica le soglie di stratificazione alle metriche quantitative."""
    if not thresholds:
        raise DomainContractError("Il dizionario delle soglie non può essere vuoto.")

    def categorize(col: str):
        q = thresholds[col]
        q_keys = sorted(q.keys())
        return (
            pl.when(pl.col(col) == 0).then(pl.lit("Zero"))
            .when(pl.col(col) <= q[q_keys[0]]).then(pl.lit(quantile_labels[0]))
            .when(pl.col(col) <= q[q_keys[1]]).then(pl.lit(quantile_labels[1]))
            .when(pl.col(col) <= q[q_keys[2]]).then(pl.lit(quantile_labels[2]))
            .otherwise(pl.lit(quantile_labels[3]))
            .alias(f"{col}_cat")
        )

    lf_with_categories = metrics_lf.with_columns([categorize(c) for c in thresholds.keys()])

    return lf_with_categories.with_columns(
        pl.concat_str(
            [pl.col(f"{c}_cat") for c in thresholds.keys()],
            separator="|"
        ).alias("strato_id")
    )


# =====================================================================
# FASE 5 — DISTRIBUZIONE DEGLI STRATI
# =====================================================================

def calculate_strata_distribution(stratified_df: pl.DataFrame) -> pl.DataFrame:
    """Calcola la distribuzione percentuale delle repository per ciascun strato."""
    if "strato_id" not in stratified_df.columns:
        raise DomainContractError("Colonna mancante: 'strato_id'.")

    if stratified_df.is_empty():
        return pl.DataFrame({"strato_id": [], "num_repo": [], "perc_tot": []})

    counts = stratified_df.group_by("strato_id").agg(pl.len().alias("num_repo"))
    total = stratified_df.height

    return (
        counts.with_columns(
            (pl.col("num_repo") / total * 100.0)
            .round(2)
            .alias("perc_tot")
        )
        .sort("num_repo", descending=True)
    )


# =====================================================================
# FASE 6 — SELEZIONE CAMPIONI RAPPRESENTATIVI
# =====================================================================

def select_representative_repos(
    stratified_df: pl.DataFrame,
    archetype_profiles: Dict[str, pl.Expr],
    sampling_metric: str = "volume_lavoro_cum"
) -> SamplingReport:
    """
    Seleziona repository rappresentative per ciascun archetipo, senza eseguire I/O.
    Restituisce una struttura dati 'SamplingReport' contenente ID e statistiche.
    """
    sampled_repo_ids: Dict[str, List[str]] = {}
    detailed_reports: Dict[str, SamplingInfo] = {}

    cat_cols = [col for col in stratified_df.columns if col.endswith('_cat')]

    for archetype_name, profile_expression in archetype_profiles.items():
        repos_in_archetype = stratified_df.filter(profile_expression)
        n_repos = len(repos_in_archetype)

        # Nessuna repository corrispondente
        if n_repos == 0:
            sampled_repo_ids[archetype_name] = []
            detailed_reports[archetype_name] = SamplingInfo(
                stats_table=pl.DataFrame(), selected_repos=[]
            )
            continue

        # Calcolo statistiche descrittive per ogni combinazione di categorie
        stats_table = (
            repos_in_archetype.group_by(cat_cols)
            .agg([
                pl.len().alias("n_repos"),
                pl.col("age_in_days").mean().alias("age_in_days_media"),
                pl.col("age_in_days").median().alias("age_in_days_mediana"),
            ])
            .sort("n_repos", descending=True)
        )

        if sampling_metric not in repos_in_archetype.columns:
            raise DomainError(f"Metrica di campionamento '{sampling_metric}' non trovata.")

        # Seleziona la repository con valore massimo della metrica per ciascuna combinazione
        top_repos_df = (
            repos_in_archetype
            .sort(sampling_metric, descending=True)
            .unique(subset=cat_cols, keep="first")
        )

        sampled_repo_ids[archetype_name] = top_repos_df.get_column("repo_id").to_list()

        selected_repos_info = (
            top_repos_df
            .with_columns(
                pl.concat_str([pl.col(c) for c in cat_cols], separator="|").alias("strato_id_combined")
            )
            .select(["repo_id", "strato_id_combined", sampling_metric])
            .to_dicts()
        )

        detailed_reports[archetype_name] = SamplingInfo(
            stats_table=stats_table,
            selected_repos=selected_repos_info
        )

    return SamplingReport(
        sampled_repo_ids=sampled_repo_ids,
        detailed_reports=detailed_reports
    )
