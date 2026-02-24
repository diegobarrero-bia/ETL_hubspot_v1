"""Entry point para el event processor: python -m processor."""
import logging
import signal
import sys

from dotenv import load_dotenv

load_dotenv(".env.webhook")

from core.logging_config import setup_logging

setup_logging("event-processor")

from core.config import WebhookConfig
from processor.worker import Worker

logger = logging.getLogger(__name__)


def main():
    """Inicializa y ejecuta el worker."""
    config = WebhookConfig()
    worker = Worker(config)

    def handle_signal(signum, frame):
        logger.info("Señal %s recibida — iniciando shutdown graceful", signum)
        worker.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info("Event processor iniciando...")
    worker.run()
    logger.info("Event processor finalizado")


if __name__ == "__main__":
    main()
