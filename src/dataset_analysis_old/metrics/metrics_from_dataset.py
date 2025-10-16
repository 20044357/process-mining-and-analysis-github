import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow as pa
from typing import Set
from ..utils import open_parquet_dataset_only, format_delta, fill_null_zero, fill_null_zero_float, scan_parquet_dataset_only, scan_parquet_dataset_only_august
import polars as pl
from typing import Set

# -----------------------------
# Helpers interni
# -----------------------------
def _calc_star_count(dset, repo_filter):
    stars_tbl = dset.to_table(
        columns=["repo_id"],
        filter=((ds.field("activity") == "WatchEvent") &
                (ds.field("action") == "started") & repo_filter)
    )
    result = stars_tbl.group_by("repo_id").aggregate(
        [("repo_id", "count")]
    ).rename_columns(["repo_id", "star_count"])
    return result.set_column(0, "repo_id", result["repo_id"].cast(pa.string()))


def _calc_fork_count(dset, repo_filter):
    forks_tbl = dset.to_table(
        columns=["repo_id"],
        filter=((ds.field("activity") == "ForkEvent") & repo_filter)
    )
    result = forks_tbl.group_by("repo_id").aggregate(
        [("repo_id", "count")]
    ).rename_columns(["repo_id", "fork_count"])
    return result.set_column(0, "repo_id", result["repo_id"].cast(pa.string()))


def _calc_contrib_count(dset, repo_filter):
    contrib_tbl = dset.to_table(columns=["repo_id", "actor_id"], filter=repo_filter)
    result = contrib_tbl.group_by("repo_id").aggregate(
        [("actor_id", "count_distinct")]
    ).rename_columns(["repo_id", "contrib_count"])
    return result.set_column(0, "repo_id", result["repo_id"].cast(pa.string()))


def _calc_repo_age(dset, repo_filter):
    age_tbl = dset.to_table(
        columns=["repo_id", "timestamp"],
        filter=((ds.field("activity") == "CreateEvent") &
                (ds.field("create_ref_type") == "repository") & repo_filter)
    )
    if age_tbl.num_rows == 0:
        return pa.table({
            "repo_id": pa.array([], type=pa.string()),
            "age_human": pa.array([], type=pa.string())
        })

    age_tbl = age_tbl.group_by("repo_id").aggregate(
        [("timestamp", "min")]
    ).rename_columns(["repo_id", "min_timestamp"])

    formatted = [format_delta(ts) for ts in age_tbl["min_timestamp"]]
    age_tbl = age_tbl.set_column(0, "repo_id", age_tbl["repo_id"].cast(pa.string()))
    age_tbl = age_tbl.append_column("age_human", pa.array(formatted, type=pa.string()))
    return age_tbl.drop_columns("min_timestamp")


def _calc_push_metrics(dset, repo_filter, repo_ids: Set[str]):
    push_tbl = dset.to_table(
        columns=["repo_id", "timestamp"],
        filter=((ds.field("activity") == "PushEvent") & repo_filter)
    )
    if push_tbl.num_rows == 0:
        return pa.table({
            "repo_id": pa.array(list(repo_ids), type=pa.string()),
            "push_count": [0] * len(repo_ids),
            "active_weeks": [0] * len(repo_ids),
            "freq_push": [0.0] * len(repo_ids)
        })

    push_count_tbl = push_tbl.group_by("repo_id").aggregate(
        [("repo_id", "count")]
    ).rename_columns(["repo_id", "push_count"])
    push_count_tbl = push_count_tbl.set_column(0, "repo_id", push_count_tbl["repo_id"].cast(pa.string()))

    df_push = push_tbl.to_pandas()
    df_push["week"] = df_push["timestamp"].dt.tz_localize(None).dt.to_period("W")
    active_weeks = df_push.groupby("repo_id")["week"].nunique()

    weeks_tbl = pa.table({
        "repo_id": pa.array([str(x) for x in active_weeks.index.to_list()], type=pa.string()),
        "active_weeks": pa.array(active_weeks.to_list(), type=pa.int32())
    })

    result_tbl = push_count_tbl.join(weeks_tbl, keys="repo_id", join_type="full outer")
    result_tbl = result_tbl.set_column(0, "repo_id", result_tbl["repo_id"].cast(pa.string()))

    push_count = fill_null_zero(result_tbl["push_count"])
    active_weeks = fill_null_zero(result_tbl["active_weeks"])

    freq_push = pc.divide(push_count.cast(pa.float32()), active_weeks.cast(pa.float32()))  # type: ignore
    freq_push = fill_null_zero_float(freq_push)

    result_tbl = result_tbl.set_column(1, "push_count", push_count)
    result_tbl = result_tbl.set_column(2, "active_weeks", active_weeks)
    result_tbl = result_tbl.append_column("freq_push", freq_push)

    return result_tbl


# -----------------------------
# Funzioni principali
# -----------------------------
def calculate_success_score_only(base_dir: str, repo_ids: Set[str]) -> pa.Table:
    """
    Calcola StarCount, ForkCount e SuccessScore per ogni repo,
    includendo anche quelle senza eventi (0 valori).
    Nota: repo_id gestiti come stringhe.
    """
    dset = open_parquet_dataset_only(base_dir)
    repo_filter = ds.field("repo_id").isin(list(repo_ids))

    # Tabelle parziali
    star_count_tbl = _calc_star_count(dset, repo_filter)
    fork_count_tbl = _calc_fork_count(dset, repo_filter)

    # Base: tutte le repo create
    metrics_tbl = pa.table({"repo_id": pa.array(list(repo_ids), type=pa.string())})

    # Join left per mantenere tutte le repo
    metrics_tbl = metrics_tbl.join(star_count_tbl, keys="repo_id", join_type="left outer")
    metrics_tbl = metrics_tbl.join(fork_count_tbl, keys="repo_id", join_type="left outer")

    # Riempi i null con 0
    star_count = fill_null_zero(metrics_tbl["star_count"])
    fork_count = fill_null_zero(metrics_tbl["fork_count"])
    success_score = pc.add(star_count, fork_count)  # type: ignore # somma semplice

    metrics_tbl = metrics_tbl.set_column(
        metrics_tbl.schema.get_field_index("star_count"), "star_count", star_count
    )
    metrics_tbl = metrics_tbl.set_column(
        metrics_tbl.schema.get_field_index("fork_count"), "fork_count", fork_count
    )
    metrics_tbl = metrics_tbl.append_column("success_score", success_score)

    return metrics_tbl


def calculate_activity_summary(base_dir: str, repo_ids: Set[str]) -> pl.DataFrame:
    """
    Calcola le metriche per le repo_id specificate,
    ma scarta quelle con num_push <= 1.
    """
    # Scan parquet (puoi usare la tua versione con filtro agosto o globale)
    lf = scan_parquet_dataset_only(base_dir).with_columns(pl.col("repo_id").cast(pl.Utf8))
    #lf = scan_parquet_dataset_only_august(base_dir).with_columns(pl.col("repo_id").cast(pl.Utf8))

    # Costruisci repo_ids come DataFrame per semi-join (RAM-friendly)
    repo_ids_lf = pl.LazyFrame({"repo_id": list(repo_ids)})

    lf = lf.join(repo_ids_lf, on="repo_id", how="semi")

    # Aggregazione
    metrics_lf = lf.group_by("repo_id").agg([
        (
            pl.when(pl.col("activity") == "PushEvent")
              .then(1).otherwise(0)
        ).sum().alias("num_push"),
        (
            pl.when((pl.col("activity") == "WatchEvent") & (pl.col("action") == "started"))
              .then(1).otherwise(0)
        ).sum().alias("num_watch"),
        pl.n_unique("actor_id").alias("num_attori"),
    ])

    metrics_df = metrics_lf.collect()

    # Scarta direttamente repo con <= 1 push
    metrics_df = metrics_df.filter(pl.col("num_push") > 1)

    return metrics_df



