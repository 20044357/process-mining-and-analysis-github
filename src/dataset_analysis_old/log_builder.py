import polars as pl
import pm4py
from typing import Dict, Any
from pm4py.objects.log.obj import EventLog


def _map_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Mappa le colonne del DataFrame ai nomi standard del process mining."""
    return df.rename({
        "actor_id": "case:concept:name",
        "timestamp": "time:timestamp",
    })


def _normalize_event_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalizza e rinomina i nomi degli eventi combinando activity e i campi payload specifici.
    Gestisce correttamente PR, Issues, Reviews, Commenti, Create/Delete, Release.
    """

    # Colonne dinamiche corrette (se mancano in certi dataset si evita crash)
    has_pr_merged = "pr_merged" in df.columns
    has_review_state = "review_state" in df.columns
    has_create_ref_type = "create_ref_type" in df.columns
    has_delete_ref_type = "delete_ref_type" in df.columns

    return df.with_columns(
        pl.when(pl.col("activity") == "PullRequestEvent")
        .then(
            pl.when((pl.col("action") == "closed") & (pl.col("pr_merged") == True) if has_pr_merged else False)
            .then(pl.lit("PullRequestEvent_merged"))
            .when((pl.col("action") == "closed") & (pl.col("pr_merged") == False) if has_pr_merged else False)
            .then(pl.lit("PullRequestEvent_rejected"))
            .otherwise(pl.col("activity").cast(pl.Utf8) + "_" + pl.col("action").cast(pl.Utf8))
        )

        .when(pl.col("activity") == "IssuesEvent")
        .then(pl.col("activity").cast(pl.Utf8) + "_" + pl.col("action").cast(pl.Utf8))

        .when(pl.col("activity") == "PullRequestReviewEvent")
        .then(
            pl.col("activity").cast(pl.Utf8) + "_" +
            (pl.col("review_state").cast(pl.Utf8) if has_review_state else pl.lit("unknown"))
        )

        .when(pl.col("activity").is_in(["IssueCommentEvent", "PullRequestReviewCommentEvent"]))
        .then(pl.col("activity").cast(pl.Utf8) + "_" + pl.col("action").cast(pl.Utf8))

        .when(pl.col("activity") == "CreateEvent")
        .then(
            pl.col("activity").cast(pl.Utf8) + "_" +
            (pl.col("create_ref_type").cast(pl.Utf8) if has_create_ref_type else pl.lit("ref"))
        )

        .when(pl.col("activity") == "DeleteEvent")
        .then(
            pl.col("activity").cast(pl.Utf8) + "_" +
            (pl.col("delete_ref_type").cast(pl.Utf8) if has_delete_ref_type else pl.lit("ref"))
        )

        .when(pl.col("activity") == "ReleaseEvent")
        .then(pl.col("activity").cast(pl.Utf8) + "_" + pl.col("action").cast(pl.Utf8))

        # default → nome evento originale
        .otherwise(pl.col("activity").cast(pl.Utf8))

        .alias("concept:name")
    )

def _format_and_select_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Forza i tipi corretti, formatta il timestamp e seleziona le colonne finali richieste da PM4Py.
    """
    df = df.with_columns(
        pl.col("case:concept:name").cast(pl.Utf8).alias("case:concept:name"),
        pl.col("concept:name").cast(pl.Utf8).alias("concept:name"),
        pl.col("case:concept:name").cast(pl.Utf8).alias("org:resource"),
        pl.col("time:timestamp").dt.replace_time_zone("UTC")
    )

    return df.select([
        "case:concept:name",
        "concept:name",
        "time:timestamp",
        "org:resource"
    ])


def build_behavior_actor_log(df: pl.DataFrame) -> EventLog | None:
    """
    Funzione principale che orchesta la costruzione del log di eventi.
    """
    if df.is_empty():
        return None

    df_mapped = _map_columns(df)
    df_renamed = _normalize_event_names(df_mapped)
    df_final = _format_and_select_columns(df_renamed)

    df_sorted = df_final.sort(["case:concept:name", "time:timestamp"])

    # Conversione finale da Polars a un EventLog PM4Py
    return pm4py.convert_to_event_log(df_sorted.to_pandas())
