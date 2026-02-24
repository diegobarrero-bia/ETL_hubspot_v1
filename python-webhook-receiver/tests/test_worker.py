"""Tests para Worker Loop (Increment 9 + 10)."""
import importlib
import threading
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from processor.batcher import WebhookEvent, EventBatch, EventBatcher
from processor.worker import Worker


def _make_poll_result(object_type: str, object_id: int) -> tuple[WebhookEvent, str]:
    """Helper: crea tupla (event, receipt_handle) para simular poll()."""
    event = WebhookEvent.from_hubspot_payload({
        "subscriptionType": f"{object_type}.propertyChange",
        "objectId": object_id,
        "occurredAt": 1700000000000,
    })
    return (event, f"receipt-{object_id}")


class TestWorkerPolling:
    """Verifica ciclo principal: poll → batch → process → acknowledge."""

    @patch("processor.worker.EventHandler")
    @patch("processor.worker.SQSConsumer")
    @patch("processor.worker.EventBatcher")
    def test_worker_polls_batches_and_processes(
        self, mock_batcher_cls, mock_consumer_cls, mock_handler_cls, webhook_config
    ):
        """Consumer retorna 3 eventos, batcher ready → handler.process_batch llamado."""
        mock_consumer = MagicMock()
        poll_results = [
            _make_poll_result("contact", 1),
            _make_poll_result("contact", 2),
            _make_poll_result("contact", 3),
        ]
        mock_consumer.poll.side_effect = [poll_results, []]
        mock_consumer_cls.return_value = mock_consumer

        # Batcher con pending_count dinámico que baja a 0 tras flush
        mock_batcher = MagicMock()
        pending = [0]

        def add_event(event):
            pending[0] += 1
        mock_batcher.add.side_effect = add_event

        type(mock_batcher).pending_count = PropertyMock(side_effect=lambda: pending[0])
        mock_batcher.is_ready.side_effect = [True]

        def do_flush():
            pending[0] = 0
            return EventBatch(events=[r[0] for r in poll_results])
        mock_batcher.flush.side_effect = do_flush
        mock_batcher_cls.return_value = mock_batcher

        mock_handler = MagicMock()
        mock_handler.process_batch.return_value = {"processed": 3, "deleted": 0, "errors": 0}
        mock_handler_cls.return_value = mock_handler

        worker = Worker(webhook_config)
        worker._stop_event.set()
        worker.run()

        mock_handler.process_batch.assert_called_once()
        mock_consumer.acknowledge.assert_called_once()

    @patch("processor.worker.EventHandler")
    @patch("processor.worker.SQSConsumer")
    @patch("processor.worker.EventBatcher")
    def test_worker_acknowledges_after_success(
        self, mock_batcher_cls, mock_consumer_cls, mock_handler_cls, webhook_config
    ):
        """Tras procesamiento exitoso, acknowledge con los receipt handles correctos."""
        mock_consumer = MagicMock()
        poll_results = [
            _make_poll_result("contact", 10),
            _make_poll_result("contact", 20),
        ]
        mock_consumer.poll.side_effect = [poll_results, []]
        mock_consumer_cls.return_value = mock_consumer

        mock_batcher = MagicMock()
        pending = [0]

        def add_event(event):
            pending[0] += 1
        mock_batcher.add.side_effect = add_event

        type(mock_batcher).pending_count = PropertyMock(side_effect=lambda: pending[0])
        mock_batcher.is_ready.side_effect = [True]

        def do_flush():
            pending[0] = 0
            return EventBatch(events=[r[0] for r in poll_results])
        mock_batcher.flush.side_effect = do_flush
        mock_batcher_cls.return_value = mock_batcher

        mock_handler = MagicMock()
        mock_handler.process_batch.return_value = {"processed": 2, "deleted": 0, "errors": 0}
        mock_handler_cls.return_value = mock_handler

        worker = Worker(webhook_config)
        worker._stop_event.set()
        worker.run()

        mock_consumer.acknowledge.assert_called_once_with(["receipt-10", "receipt-20"])

    @patch("processor.worker.EventHandler")
    @patch("processor.worker.SQSConsumer")
    @patch("processor.worker.EventBatcher")
    def test_worker_does_not_acknowledge_on_failure(
        self, mock_batcher_cls, mock_consumer_cls, mock_handler_cls, webhook_config
    ):
        """Si handler.process_batch falla, NO se hace acknowledge."""
        mock_consumer = MagicMock()
        poll_results = [_make_poll_result("contact", 1)]
        mock_consumer.poll.side_effect = [poll_results, []]
        mock_consumer_cls.return_value = mock_consumer

        mock_batcher = MagicMock()
        mock_batcher.pending_count = 1
        mock_batcher.is_ready.side_effect = [True]
        mock_batcher.flush.return_value = EventBatch(events=[poll_results[0][0]])
        mock_batcher_cls.return_value = mock_batcher

        mock_handler = MagicMock()
        mock_handler.process_batch.side_effect = Exception("DB connection lost")
        mock_handler_cls.return_value = mock_handler

        worker = Worker(webhook_config)
        worker._stop_event.set()
        worker.run()

        mock_consumer.acknowledge.assert_not_called()


class TestWorkerShutdown:
    """Verifica shutdown graceful."""

    @patch("processor.worker.EventHandler")
    @patch("processor.worker.SQSConsumer")
    @patch("processor.worker.EventBatcher")
    def test_worker_stops_on_shutdown_signal(
        self, mock_batcher_cls, mock_consumer_cls, mock_handler_cls, webhook_config
    ):
        """Set stop event → loop termina dentro de un ciclo."""
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = []
        mock_consumer_cls.return_value = mock_consumer

        mock_batcher = MagicMock()
        mock_batcher.pending_count = 0
        mock_batcher.is_ready.return_value = False
        mock_batcher_cls.return_value = mock_batcher

        mock_handler_cls.return_value = MagicMock()

        worker = Worker(webhook_config)
        worker.stop()
        worker.run()

        assert mock_consumer.poll.call_count <= 1

    @patch("processor.worker.EventHandler")
    @patch("processor.worker.SQSConsumer")
    @patch("processor.worker.EventBatcher")
    def test_worker_flushes_remaining_on_shutdown(
        self, mock_batcher_cls, mock_consumer_cls, mock_handler_cls, webhook_config
    ):
        """Eventos pendientes en batcher al recibir stop → se procesan antes de salir."""
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = []
        mock_consumer_cls.return_value = mock_consumer

        remaining_event = WebhookEvent.from_hubspot_payload({
            "subscriptionType": "contact.propertyChange",
            "objectId": 42,
            "occurredAt": 1700000000000,
        })

        mock_batcher = MagicMock()
        mock_batcher.pending_count = 1
        mock_batcher.is_ready.return_value = False
        mock_batcher.flush.return_value = EventBatch(events=[remaining_event])
        mock_batcher_cls.return_value = mock_batcher

        mock_handler = MagicMock()
        mock_handler.process_batch.return_value = {"processed": 1, "deleted": 0, "errors": 0}
        mock_handler_cls.return_value = mock_handler

        worker = Worker(webhook_config)
        worker.stop()
        worker.run()

        mock_handler.process_batch.assert_called_once()
        mock_batcher.flush.assert_called_once()


class TestWorkerInit:
    """Verifica inicialización y entry point (Increment 10)."""

    @patch("processor.worker.EventHandler")
    @patch("processor.worker.SQSConsumer")
    @patch("processor.worker.EventBatcher")
    def test_worker_init_from_config(
        self, mock_batcher_cls, mock_consumer_cls, mock_handler_cls, webhook_config
    ):
        """Construye Worker desde WebhookConfig; sub-componentes inicializados."""
        worker = Worker(webhook_config)

        mock_consumer_cls.assert_called_once_with(
            queue_url=webhook_config.sqs_queue_url,
            region=webhook_config.sqs_region,
            wait_seconds=webhook_config.poll_wait_seconds,
        )
        mock_batcher_cls.assert_called_once_with(
            max_batch_size=webhook_config.batch_max_events,
            batch_wait_seconds=webhook_config.batch_wait_seconds,
        )
        mock_handler_cls.assert_called_once_with(webhook_config)

    def test_processor_entrypoint_importable(self):
        """Verifica que processor.__main__ se puede importar sin errores."""
        mod = importlib.import_module("processor.__main__")
        assert hasattr(mod, "main")
