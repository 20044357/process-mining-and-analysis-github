import os
import json
import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict, Any

class DailyWriter:
    """
    Scrittore giornaliero di eventi distillati.

    - Appende eventi a file JSONL.
    - Gestisce chiusure sicure (ok/abort).
    - Converte file JSONL in dataset Parquet partizionato.
    """

    def __init__(self, output_path: str):
        """
        Inizializza il writer.

        Args:
            output_path (str): Percorso file events.jsonl.
        """
        self.output_path = output_path
        self.file = None
        self.written = 0

    def write_event(self, event: Dict[str, Any]):
        """
        Chiude il file in caso di completamento regolare.
        """
        if not self.file:
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            self.file = open(self.output_path, "a", encoding="utf-8")
        self.file.write(json.dumps(event) + "\n")
        self.written += 1

    def close_ok(self):
        if self.file:
            self.file.close()

    def close_abort(self):
        """
        Chiude e rimuove il file in caso di errore.
        """
        if self.file:
            self.file.close()
            os.remove(self.output_path)

    def convert_to_parquet(self, parquet_dir: str):
        """
        Converte il file JSONL in formato Parquet, partizionato per anno/mese/giorno.

        Args:
            parquet_dir (str): Directory root per salvare i file Parquet.

        Returns:
            bool: True se conversione riuscita, False altrimenti.
        """
        try:
            df = pd.read_json(self.output_path, lines=True)

            df["anno"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y")
            df["mese"] = pd.to_datetime(df["timestamp"]).dt.strftime("%m")
            df["giorno"] = pd.to_datetime(df["timestamp"]).dt.strftime("%d")

            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_to_dataset(
                table,
                root_path=parquet_dir,
                partition_cols=["anno", "mese", "giorno"],
            )
            print(f"[OK] Dati convertiti in Parquet e salvati in {parquet_dir}")
            os.remove(self.output_path)
            return True
        except Exception as e:
            print(f"[ERROR]: Errore durante la conversione in Parquet: {e}", file=sys.stderr)
            return False           
