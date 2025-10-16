from pathlib import Path
from typing import Dict
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow as pa
from datetime import datetime, timezone
import polars as pl
from pathlib import Path

def scan_parquet_dataset_only(base_dir: str) -> pl.LazyFrame:
    """
    Raccoglie SOLO i file .parquet sotto base_dir e costruisce un LazyFrame Polars.
    Evita che Polars inciampi su file non-parquet.
    Restituisce un LazyFrame per un'elaborazione efficiente.
    """
    # 1. Trova tutti i file .parquet in modo ricorsivo (come prima)
    files = [str(p) for p in Path(base_dir).rglob("*.parquet")]

    # 2. Gestisci il caso in cui non vengono trovati file (come prima)
    if not files:
        raise FileNotFoundError(f"Nessun file .parquet trovato sotto {base_dir}")

    # 3. Usa pl.scan_parquet con la lista di file per creare un LazyFrame
    # Questa è la modifica chiave: la funzione ora restituisce un oggetto Polars.
    return pl.scan_parquet(files)


def scan_parquet_dataset_only_august(base_dir: str) -> pl.LazyFrame:
    """
    Versione temporanea: raccoglie SOLO i file .parquet di agosto (mese=08)
    sotto base_dir e costruisce un LazyFrame Polars.
    """
    # 1. Trova tutti i file parquet con "08" nel nome o nel path
    files = [str(p) for p in Path(base_dir).rglob("*.parquet") if "08" in p.name]

    # 2. Se non ci sono file, segnala errore
    if not files:
        raise FileNotFoundError(f"Nessun file .parquet di agosto trovato sotto {base_dir}")

    # 3. LazyFrame
    return pl.scan_parquet(files)


def open_parquet_dataset_only(base_dir: str) -> ds.Dataset:
    """
    Raccoglie SOLO i file .parquet sotto base_dir e costruisce un Dataset Arrow.
    Evita che Arrow inciampi su index.json / events.jsonl.
    """
    files = [str(p) for p in Path(base_dir).rglob("*.parquet")]
    if not files:
        raise FileNotFoundError(f"Nessun .parquet trovato sotto {base_dir}")
    return ds.dataset(files, format="parquet")


def load_repo_strata(stratified_path: str) -> Dict[str, int]:
    df = pl.read_csv(stratified_path).with_columns(pl.col("repo_id").cast(pl.Utf8))
    strata_map = dict(zip(df["repo_id"].to_list(), df["strato_id"].to_list()))
    return strata_map


def format_delta(ts):
    """
    Converte un timestamp Arrow in stringa 'Xg Yh Zm Ws'.
    """
    now_ts = datetime.now(timezone.utc)
    delta = now_ts - ts.as_py()
    total_seconds = int(delta.total_seconds())
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{days}g {hours}h {minutes}m {seconds}s"

def fill_null_zero(array, dtype=pa.int64()):
    """Sostituisce null con 0, castando a int."""
    return pc.fill_null(array.cast(dtype), 0)

def fill_null_zero_float(array):
    """Sostituisce null con 0.0, castando a float."""
    return pc.fill_null(array.cast(pa.float32()), 0.0)
