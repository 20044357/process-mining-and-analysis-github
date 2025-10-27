# File: src/dataset_analysis/infrastructure/logging_config.py

import logging
from typing import Any, MutableMapping


# ============================================================
# CONFIGURAZIONE GLOBALE DEL LOGGING
# ============================================================

def configure_logging() -> None:
    """
    Configura il formato e il livello di logging utilizzato in tutti i layer del progetto.

    Definisce un formato coerente e leggibile per i messaggi di log,
    valido per ogni adapter o componente. Il formato evita l’uso di
    variabili personalizzate (come %(layer)s) che potrebbero generare
    errori con logger esterni o librerie terze.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)-8s - [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# ============================================================
# LOGGER ADATTATO PER I LAYER
# ============================================================

class LayerLoggerAdapter(logging.LoggerAdapter):
    """
    Adapter per il sistema di logging che aggiunge in modo sicuro
    il nome del layer architetturale ai messaggi di log.

    Questo logger è utilizzato da tutti gli adapter principali
    (Application, Domain, Infrastructure) per fornire un contesto
    uniforme e facilmente tracciabile all’interno dei log.
    """

    def process(
        self,
        msg: Any,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[Any, MutableMapping[str, Any]]:
        """
        Prepara il messaggio di log aggiungendo il prefisso del layer.

        Args:
            msg: Messaggio di log originale.
            kwargs: Parametri addizionali passati al logger.

        Returns:
            Una tupla contenente il messaggio formattato e i parametri invariati.
        """
        layer_name = self.extra.get("layer", "Generic") if self.extra else "Generic"
        return f"[{layer_name}] {msg}", kwargs
