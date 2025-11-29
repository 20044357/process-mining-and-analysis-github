from datetime import datetime
import numpy as np
import pandas as pd
import polars as pl
from typing import Dict, List

from .predicates import is_bot_actor
from .errors import DomainContractError, CalculationError

def extract_analyzable_repository(raw_events_df: pl.DataFrame) -> pl.DataFrame:
    required = {"repo_id", "timestamp"}
    missing = required - set(raw_events_df.columns)

    if missing:
        raise DomainContractError(f"Colonne mancanti nel DataFrame: {missing}")

    if raw_events_df.is_empty():
        return pl.DataFrame(schema={"repo_id": pl.Utf8, "repo_creation_date": pl.Datetime(time_unit="us", time_zone="UTC")})

    return (
        raw_events_df
        .rename({"timestamp": "repo_creation_date"})
        .unique(subset=["repo_id"], keep="first")
        .with_columns(
            pl.col("repo_creation_date").dt.replace_time_zone("UTC").alias("repo_creation_date")
        )
    )


def calculate_metrics_for_repository(
    base_events_lf: pl.LazyFrame,
    repo_creation_lookup_df: pl.DataFrame,
    popularity_predicate: pl.Expr,
    engagement_predicate: pl.Expr,
    collaboration_predicate: pl.Expr,
    analysis_end_date: str
) -> pl.LazyFrame:
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
            f"La data di fine analisi '{analysis_end_date}' non è in formato valido."
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
            pl.sum("push_volume").alias("workload_cum"),
            pl.sum("popularity_event_count").alias("external_popularity_cum"),
            pl.sum("engagement_event_count").alias("community_engagement_cum"),
        ])
    )

    collaborator_counts_lf = (
        relevant_events_lf.filter(collaboration_predicate)
        .group_by("repo_id")
        .agg(pl.col("actor_id").n_unique().alias("collaboration_intensity_cum"))
    )

    final_metrics_lf = (
        cumulative_sums_lf
        .join(collaborator_counts_lf, on="repo_id", how="left")
        .join(repo_creation_lookup_lf, on="repo_id", how="inner")
        .with_columns(
            pl.col("collaboration_intensity_cum").fill_null(0),
            pl.col("repo_creation_date").dt.replace_time_zone("UTC").alias("repo_creation_date"),
                    
            (
                pl.lit(analysis_end_dt).cast(pl.Datetime(time_unit="us", time_zone="UTC")) 
                - pl.col("repo_creation_date")
            )
                .dt.total_days().alias("age_in_days")
        ) 
        .with_columns([
            (pl.col("workload_cum") / pl.col("age_in_days")).fill_null(0).alias("workload_norm"),
            (pl.col("collaboration_intensity_cum") / pl.col("age_in_days")).fill_null(0).alias("collaboration_intensity_norm"),
            (pl.col("external_popularity_cum") / pl.col("age_in_days")).fill_null(0).alias("external_popularity_norm"),
            (pl.col("community_engagement_cum") / pl.col("age_in_days")).fill_null(0).alias("community_engagement_norm"),
        ])
        .filter(pl.col("age_in_days") > 0)
    )

    return final_metrics_lf


def compute_quantiles_for_metrics(
    metrics_lf: pl.LazyFrame,
    quantiles_to_compute: List[float]
) -> Dict[str, Dict[str, float]]:
    METRICS = [
        "workload_cum", "collaboration_intensity_cum",
        "community_engagement_cum", "external_popularity_cum",
        "workload_norm", "collaboration_intensity_norm",
        "community_engagement_norm", "external_popularity_norm",
    ]

    try:
        if metrics_lf.limit(1).collect().is_empty():
            raise CalculationError("Il LazyFrame delle metriche è vuoto dopo il filtro.")

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
                f"Q{int(q * 100)}": float(
                    value if (value := result_dict.get(f"{col}_Q{int(q * 100)}")) is not None else 0.0
                )
                for q in quantiles_to_compute
            }
            for col in METRICS
        }
        if not thresholds:
            raise CalculationError("Risultato vuoto nel calcolo delle soglie di stratificazione.")

        return thresholds

    except Exception as e:
        raise CalculationError(f"Errore nel calcolo delle soglie: {e}") from e
    

def classify_repository(
    metrics_lf: pl.LazyFrame,
    thresholds: Dict[str, Dict[str, float]],
    quantile_labels: List[str]
) -> pl.LazyFrame:
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

def report_archetype_distribution(stratified_df: pl.DataFrame) -> pl.DataFrame:
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