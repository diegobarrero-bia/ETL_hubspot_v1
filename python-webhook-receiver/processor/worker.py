"""Worker loop principal para el event processor."""
import logging
import threading

from core.config import WebhookConfig
from processor.batcher import EventBatcher
from processor.event_handler import EventHandler
from processor.sqs_consumer import SQSConsumer

logger = logging.getLogger(__name__)


class Worker:
    """Orquesta el ciclo: poll SQS → batch → process → acknowledge."""

    def __init__(self, config: WebhookConfig):
        self.config = config
        self._stop_event = threading.Event()
        self._consumer = SQSConsumer(
            queue_url=config.sqs_queue_url,
            region=config.sqs_region,
            wait_seconds=config.poll_wait_seconds,
        )
        self._batcher = EventBatcher(
            max_batch_size=config.batch_max_events,
            batch_wait_seconds=config.batch_wait_seconds,
        )
        self._handler = EventHandler(config)
        self._pending_handles: list[str] = []

    def run(self) -> None:
        """Ejecuta el loop principal hasta recibir señal de stop."""
        logger.info("Worker iniciado — esperando eventos de SQS")

        while True:
            try:
                self._poll_and_process()
            except Exception as e:
                logger.error("Error en ciclo del worker: %s", e, exc_info=True)

            if self._stop_event.is_set():
                break

        # Flush pendientes al hacer shutdown
        self._flush_remaining()
        logger.info("Worker detenido")

    def stop(self) -> None:
        """Señala al worker que debe detenerse."""
        logger.info("Señal de stop recibida")
        self._stop_event.set()

    def _poll_and_process(self) -> None:
        """Un ciclo: poll → batch → (si ready) process → acknowledge."""
        poll_results = self._consumer.poll()

        for event, receipt_handle in poll_results:
            self._batcher.add(event)
            self._pending_handles.append(receipt_handle)

        if self._batcher.is_ready():
            batch = self._batcher.flush()
            handles_to_ack = list(self._pending_handles)
            self._pending_handles.clear()
            try:
                summary = self._handler.process_batch(batch)
                logger.info(
                    "Batch procesado: %d processed, %d deleted, %d errors",
                    summary["processed"], summary["deleted"], summary["errors"],
                )
                self._consumer.acknowledge(handles_to_ack)
            except Exception as e:
                logger.error(
                    "Error procesando batch (%d eventos): %s",
                    len(batch.events), e, exc_info=True,
                )

    def _flush_remaining(self) -> None:
        """Procesa eventos pendientes en el batcher antes de salir."""
        if self._batcher.pending_count > 0:
            logger.info(
                "Flush de %d eventos pendientes al shutdown",
                self._batcher.pending_count,
            )
            batch = self._batcher.flush()
            handles_to_ack = list(self._pending_handles)
            self._pending_handles.clear()
            try:
                self._handler.process_batch(batch)
                self._consumer.acknowledge(handles_to_ack)
            except Exception as e:
                logger.error("Error en flush final: %s", e, exc_info=True)
