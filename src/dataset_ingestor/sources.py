import time
import requests
import gzip
from typing import Iterator
from requests.exceptions import Timeout, ConnectionError as ConnErr, RequestException


class LineSource:
    """
    Gestore per scaricare file GHArchive (.json.gz) e leggerli riga per riga.

    Implementa retry automatico con backoff esponenziale.
    """

    def __init__(self, timeout: int, max_retries: int, backoff_base: float):
        """
        Inizializza la sorgente di linee.

        Args:
            timeout (int): Timeout per richieste HTTP.
            max_retries (int): Numero massimo di retry.
            backoff_base (float): Base per il backoff esponenziale.
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base

    def iter_lines(self, url: str) -> Iterator[str]:
        """
        Itera sulle righe testuali di un file gzippato remoto.

        Procedura:
            - Scarica il file .gz da URL.
            - Decomprime e restituisce le righe in streaming.
            - Gestisce retry con backoff in caso di errori HTTP.

        Args:
            url (str): URL del file gharchive.org (formato YYYY-MM-DD-HH.json.gz).

        Yields:
            str: Riga di testo decodificata.
        """
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, stream=True, timeout=self.timeout)
                response.raise_for_status()
                with gzip.GzipFile(fileobj=response.raw, mode="rb") as gz_file:
                    for line in gz_file:
                        yield line.decode("utf-8")
                return
            except (Timeout, ConnErr, RequestException) as error:
                print(f"[HTTP Error] Tentativo {attempt + 1}/{self.max_retries}: {error}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.backoff_base ** (attempt + 1))
                else:
                    raise error
