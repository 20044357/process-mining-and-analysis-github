import logging
import time
import requests
import gzip
from typing import Iterator
from requests.exceptions import Timeout, ConnectionError as ConnErr, RequestException, HTTPError

from ..domain.interfaces import IEventSource
from ..application.errors import DataSourceError
from .logging_config import LayerLoggerAdapter

class GhArchiveEventSource(IEventSource):
    """Adapter che implementa IEventSource per scaricare dati da GHArchive."""
    
    def __init__(self, timeout: int = 60, max_retries: int = 3, backoff_base: float = 1.5):
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        base_logger = logging.getLogger(self.__class__.__name__)
        self.logger = LayerLoggerAdapter(base_logger, {"layer": "Infrastructure"})

    def iter_lines(self, url: str) -> Iterator[str]:
        """Itera sulle righe di un file .json.gz remoto con logica di retry."""
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, stream=True, timeout=self.timeout)
                response.raise_for_status()
                with gzip.GzipFile(fileobj=response.raw, mode="rb") as gz_file:
                    for line in gz_file:
                        yield line.decode("utf-8")
                return
            except (Timeout, ConnErr, RequestException, HTTPError) as error:
                self.logger.warning(f"Tentativo {attempt + 1}/{self.max_retries} fallito per {url}. Causa: {error}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.backoff_base ** (attempt + 1))
                else:
                    raise DataSourceError(f"Impossibile scaricare {url} dopo {self.max_retries} tentativi.") from error

