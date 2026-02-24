"""Configuración centralizada de logging JSON estructurado."""
import logging
import os
import sys

from pythonjsonlogger.json import JsonFormatter


def setup_logging(service: str = "webhook-receiver"):
    """Configura logging JSON en el root logger.

    Args:
        service: Nombre del servicio incluido en cada línea de log.
    """
    formatter = JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(funcName)s %(lineno)d %(message)s",
        rename_fields={"asctime": "timestamp", "levelname": "level"},
        static_fields={"service": service},
    )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
