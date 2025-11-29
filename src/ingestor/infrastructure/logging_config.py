import logging
from typing import Any, MutableMapping

def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)-8s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

class LayerLoggerAdapter(logging.LoggerAdapter):
    def process(
        self,
        msg: Any,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[Any, MutableMapping[str, Any]]:
        layer_name = self.extra.get("layer", "Generic") if self.extra else "Generic"
        return f"[{layer_name}] {msg}", kwargs