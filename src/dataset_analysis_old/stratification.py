import polars as pl
import json
from pathlib import Path
from typing import Dict, Any


# ------------------------------
# Step 2.1 – Calcolo soglie
# ------------------------------
def calculate_thresholds(metrics_path: str, output_path: str = "stratification_thresholds.json") -> Dict[str, Any]:
    """
    Calcola le mediane delle metriche (num_push, num_attori, num_watch)
    e salva i valori in JSON per riproducibilità.
    """
    df = pl.read_parquet(metrics_path) if metrics_path.endswith(".parquet") else pl.read_csv(metrics_path)

    soglie = df.select(
        pl.col("num_push").median().alias("med_push"),
        pl.col("num_attori").median().alias("med_attori"),
        pl.col("num_watch").median().alias("med_watch"),
    ).row(0, named=True)

    with open(output_path, "w") as f:
        json.dump(soglie, f, indent=2)

    print(f"[OK] Soglie calcolate e salvate in {output_path}: {soglie}")
    return soglie


# ------------------------------
# Step 2.2 – Etichettatura
# ------------------------------
def stratify_repositories(metrics_path: str, soglie: Dict[str, Any], output_path: str = "repo_metrics_stratified.parquet") -> pl.DataFrame:
    """
    Aggiunge una colonna strato_id (1..8) a ogni repo in base alle soglie.
    Salva il risultato in Parquet/CSV.
    """
    df = pl.read_parquet(metrics_path) if metrics_path.endswith(".parquet") else pl.read_csv(metrics_path)

    med_push, med_attori, med_watch = soglie["med_push"], soglie["med_attori"], soglie["med_watch"]

    stratified_df = (
        df
        .with_columns([
            (pl.col("num_push")   > med_push).cast(pl.Int8).alias("push_hi"),
            (pl.col("num_attori") > med_attori).cast(pl.Int8).alias("collab_hi"),
            (pl.col("num_watch")  > med_watch).cast(pl.Int8).alias("pop_hi"),
        ])
        .with_columns(
            (pl.lit(1)
             + pl.col("push_hi")*4
             + pl.col("collab_hi")*2
             + pl.col("pop_hi")*1
            ).alias("strato_id")
        )
        .drop(["push_hi", "collab_hi", "pop_hi"])
    )

    # Salvataggio (sia Parquet che CSV per ispezione)
    stratified_df.write_parquet(output_path)
    stratified_df.write_csv(output_path.replace(".parquet", ".csv"))

    print(f"[OK] Repo stratificate salvate in {output_path}")
    return stratified_df


# ------------------------------
# Step 2.3 – Campionamento
# ------------------------------
def sample_stratified_repos(
    stratified_df: pl.DataFrame,
    n_per_strato: int = 5,
    seed: int = 42,
    output_prefix: str = "campione_stratificato"
) -> pl.DataFrame:
    """
    Estrae un campione stratificato equo dalle repo etichettate.
    Salva sia il campione completo sia la lista degli ID in CSV/TXT.
    """

    # Usa map_groups invece di .sample()
    df_sample = stratified_df.group_by("strato_id").map_groups(
        lambda g: g.sample(n=min(n_per_strato, g.height), seed=seed)
    )

    # Salva campione
    df_sample.write_parquet(f"{output_prefix}.parquet")
    df_sample.write_csv(f"{output_prefix}.csv")

    # Salva lista repo_id
    repo_ids = df_sample["repo_id"].to_list()
    with open(f"{output_prefix}_ids.txt", "w") as f:
        for rid in repo_ids:
            f.write(f"{rid}\n")

    print(f"[OK] Campione stratificato salvato in {output_prefix}.parquet/.csv e lista ID in {output_prefix}_ids.txt")

    return df_sample


from typing import Optional

def count_repos_per_stratum(
    stratified_df: pl.DataFrame,
    include_empty: bool = False,
    save_csv: Optional[str] = None
) -> pl.DataFrame:
    """
    Restituisce il conteggio di repository per ciascun strato.
    - include_empty=True: riempie anche gli strati mancanti (1..8) con 0.
    - save_csv: se passato, salva il risultato in CSV nel path indicato.
    """
    # Conteggio reale per gli strati presenti
    counts = (
        stratified_df
        .group_by("strato_id")
        .agg(pl.count().alias("num_repo"))
    )

    if include_empty:
        # Crea dataframe con tutti gli 8 strati
        all_strata = pl.DataFrame({"strato_id": list(range(1, 9))})
        # Left join per includere strati assenti con 0
        counts = (
            all_strata
            .join(counts, on="strato_id", how="left")
            .with_columns(pl.col("num_repo").fill_null(0))
        )

    # Ordina per strato e aggiunge (opzionale) % sul totale
    total = counts["num_repo"].sum() if counts.height > 0 else 0
    counts = counts.with_columns(
        (pl.when(pl.lit(total) > 0)
           .then((pl.col("num_repo") / pl.lit(total) * 100.0))
           .otherwise(0.0)
        ).round(2).alias("perc_tot")
    ).sort("strato_id")

    if save_csv:
        counts.write_csv(save_csv)

    return counts

# ------------------------------
# Pipeline Step 2 completa
# ------------------------------
def run_stratification(
    metrics_path: str = "data/dataset_analyzed/metrics.csv",
    thresholds_path: str = "data/dataset_analyzed/stratification_thresholds.json",
    stratified_path: str = "data/dataset_analyzed/repo_metrics_stratified.parquet",
    n_per_strato: int = 5
):
    # 1. Calcola soglie
    soglie = calculate_thresholds(metrics_path, thresholds_path)

    # 2. Etichetta tutte le repo
    stratified_df = stratify_repositories(metrics_path, soglie, stratified_path)


    # >>> Report distribuzione per strato (1..8), salvato accanto ai file
    counts_csv = Path(stratified_path).with_name("strata_counts.csv")
    counts = count_repos_per_stratum(stratified_df, include_empty=True, save_csv=str(counts_csv))
    print("[OK] Distribuzione per strato:")
    print(counts)

    # 3. Campiona bilanciato
    sample = sample_stratified_repos(stratified_df, n_per_strato=n_per_strato)

    return sample