import logging
import time
import requests
import gzip
import json
from typing import Iterator, Dict, Any
from requests.exceptions import Timeout, ConnectionError as ConnErr, RequestException, HTTPError

from ..domain.interfaces import IEventSource
from ..application.errors import DataSourceError

class GhArchiveEventSource(IEventSource):
    
    def __init__(self, timeout: int = 60, max_retries: int = 3, backoff_base: float = 1.5):
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.logger = logging.getLogger(self.__class__.__name__)

    def _iter_lines(self, url: str) -> Iterator[str]:
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
                    raise DataSourceError(f"Impossibile scaricare {url}") from error

    def iter_events(self, url: str) -> Iterator[Dict[str, Any]]:
        for line in self._iter_lines(url):
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                self.logger.debug(f"Saltata linea malformata in {url}")
                continue