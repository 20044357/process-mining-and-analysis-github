from pathlib import Path
from datetime import datetime, timedelta
import polars as pl

def scan_parquet_dataset_only(base_dir: str, start_date: str, end_date: str) -> pl.LazyFrame:
    """
    Raccoglie SOLO i file .parquet del range temporale richiesto e costruisce
    un LazyFrame Polars efficiente, costruendo i path direttamente.
    La struttura attesa è: anno=YYYY/mese=MM/giorno=DD/....
    """

    start_dt = datetime.fromisoformat(start_date.replace("Z", "")).date()
    end_dt = datetime.fromisoformat(end_date.replace("Z", "")).date()

    current_date = start_dt
    date_paths = []

    # Genera i percorsi per ogni giorno nel range, sfruttando la struttura partizionata
    while current_date <= end_dt:
        # Costruisce il pattern del percorso per il giorno specifico
        # Esempio: "base_dir/anno=2023/mese=01/giorno=01/*.parquet"
        daily_path_pattern = Path(base_dir) / \
                             f"anno={current_date.year}" / \
                             f"mese={current_date.month:02d}" / \
                             f"giorno={current_date.day:02d}" / \
                             "*.parquet"
        date_paths.append(str(daily_path_pattern))
        current_date += timedelta(days=1)

    if not date_paths:
        raise FileNotFoundError(
            f"Nessun pattern di file generato in {base_dir} nel range {start_date} - {end_date}"
        )

    print(f"[Utils] Generati {len(date_paths)} pattern di percorsi Parquet nel range richiesto ({start_date} → {end_date}).")
    
    # Polars è molto efficiente nell'espandere questi wildcard e scansionare i file
    return pl.scan_parquet(date_paths)