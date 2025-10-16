import json
from pathlib import Path
from typing import Set
import pyarrow.dataset as ds
from .utils import open_parquet_dataset_only, scan_parquet_dataset_only, scan_parquet_dataset_only_august
import polars as pl

def extract_create_repo_events(base_dir: str) -> Set[str]:
    """
    Ritorna l'insieme dei repo_id 'nativi' (CreateEvent su repository).
    Usa Polars LazyFrame per leggere solo agosto (scan_parquet_dataset_only_august).
    """
    #dset = scan_parquet_dataset_only(base_dir)
    dset = scan_parquet_dataset_only_august(base_dir)

    required = {"repo_id", "activity", "create_ref_type"}
    names = set(dset.collect_schema().names())
    missing = required - names
    if missing:
        raise ValueError(f"Mancano colonne {sorted(missing)}. Presenti: {sorted(names)}")

    # Filtro sugli eventi di creazione repo
    filt = (pl.col("activity") == "CreateEvent") & (pl.col("create_ref_type") == "repository")

    table = (
        dset
        .filter(filt)
        .select("repo_id")
        .unique()   # elimina duplicati
        .collect()
    )

    # Converte in set di stringhe
    return set(table["repo_id"].cast(pl.Utf8).to_list())
