import logging
from typing import Any, MutableMapping

def configure_logging() -> None:
    """
    Configura il formato e il livello di logging utilizzato in tutti i layer del progetto.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)-8s - [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

class LayerLoggerAdapter(logging.LoggerAdapter):
    """
    Adapter per il sistema di logging che aggiunge il nome del layer
    architetturale ai messaggi di log in modo sicuro.
    """
    def process(
        self,
        msg: Any,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[Any, MutableMapping[str, Any]]:
        """Prepara il messaggio di log aggiungendo il prefisso del layer."""
        layer_name = self.extra.get("layer", "Generic") if self.extra else "Generic"
        return f"[{layer_name}] {msg}", kwargs