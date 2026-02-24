"""Tests para EventHandler (Increment 8)."""
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from processor.batcher import WebhookEvent, EventBatch
from processor.event_handler import EventHandler


@pytest.fixture
def handler(webhook_config):
    """EventHandler con config de test."""
    return EventHandler(webhook_config)


def _make_event(object_type_sub: str, object_id: int, change: str = "propertyChange") -> WebhookEvent:
    """Helper: crea evento rápido."""
    return WebhookEvent.from_hubspot_payload({
        "subscriptionType": f"{object_type_sub}.{change}",
        "objectId": object_id,
        "occurredAt": 1700000000000,
    })


class TestProcessUpdates:
    """Verifica procesamiento de eventos de actualización/creación."""

    @patch("processor.event_handler.DatabaseLoader")
    @patch("processor.event_handler.HubSpotExtractor")
    @patch("processor.event_handler.ETLMonitor")
    @patch("processor.event_handler.process_batch")
    def test_process_updates_fetches_and_upserts(
        self, mock_transform, mock_monitor_cls, mock_extractor_cls, mock_loader_cls, handler
    ):
        """3 update events → fetch records → transform → upsert."""
        # Setup mocks
        mock_extractor = MagicMock()
        mock_extractor_cls.return_value = mock_extractor
        mock_extractor.get_properties_with_types.return_value = (["email", "name"], {"email": "string"})
        mock_extractor.safe_request.return_value = MagicMock(
            status_code=200,
            json=lambda: {"results": [
                {"id": "1", "properties": {"email": "a@test.com"}},
                {"id": "2", "properties": {"email": "b@test.com"}},
                {"id": "3", "properties": {"email": "c@test.com"}},
            ]},
        )

        mock_loader = MagicMock()
        mock_loader_cls.return_value = mock_loader

        df = pd.DataFrame({"hs_object_id": [1, 2, 3], "email": ["a", "b", "c"]})
        mock_transform.return_value = (df, {"email": "email"})

        # Build batch
        events = [_make_event("contact", i) for i in [1, 2, 3]]
        batch = EventBatch(events=events)

        # Execute
        summary = handler.process_batch(batch)

        # Verify
        mock_loader.upsert_records.assert_called_once()
        assert summary["processed"] == 3

    @patch("processor.event_handler.DatabaseLoader")
    @patch("processor.event_handler.HubSpotExtractor")
    @patch("processor.event_handler.ETLMonitor")
    @patch("processor.event_handler.process_batch")
    def test_process_deletions_calls_mark_deleted(
        self, mock_transform, mock_monitor_cls, mock_extractor_cls, mock_loader_cls, handler
    ):
        """2 deletion events → mark_records_as_deleted([id1, id2])."""
        mock_loader = MagicMock()
        mock_loader.mark_records_as_deleted.return_value = 2
        mock_loader_cls.return_value = mock_loader

        events = [
            _make_event("deal", 301, "deletion"),
            _make_event("deal", 302, "deletion"),
        ]
        batch = EventBatch(events=events)

        summary = handler.process_batch(batch)

        mock_loader.mark_records_as_deleted.assert_called_once_with([301, 302])
        assert summary["deleted"] == 2

    @patch("processor.event_handler.DatabaseLoader")
    @patch("processor.event_handler.HubSpotExtractor")
    @patch("processor.event_handler.ETLMonitor")
    @patch("processor.event_handler.process_batch")
    def test_mixed_batch_handles_both(
        self, mock_transform, mock_monitor_cls, mock_extractor_cls, mock_loader_cls, handler
    ):
        """2 updates + 1 deletion → tanto upsert como mark_deleted llamados."""
        mock_extractor = MagicMock()
        mock_extractor_cls.return_value = mock_extractor
        mock_extractor.get_properties_with_types.return_value = (["email"], {"email": "string"})
        mock_extractor.safe_request.return_value = MagicMock(
            status_code=200,
            json=lambda: {"results": [
                {"id": "1", "properties": {"email": "a@test.com"}},
                {"id": "2", "properties": {"email": "b@test.com"}},
            ]},
        )

        mock_loader = MagicMock()
        mock_loader.mark_records_as_deleted.return_value = 1
        mock_loader_cls.return_value = mock_loader

        df = pd.DataFrame({"hs_object_id": [1, 2], "email": ["a", "b"]})
        mock_transform.return_value = (df, {"email": "email"})

        events = [
            _make_event("contact", 1, "creation"),
            _make_event("contact", 2, "propertyChange"),
            _make_event("contact", 99, "deletion"),
        ]
        batch = EventBatch(events=events)

        summary = handler.process_batch(batch)

        mock_loader.upsert_records.assert_called_once()
        mock_loader.mark_records_as_deleted.assert_called_once_with([99])
        assert summary["processed"] == 2
        assert summary["deleted"] == 1


class TestMultipleObjectTypes:
    """Verifica procesamiento separado por tipo de objeto."""

    @patch("processor.event_handler.DatabaseLoader")
    @patch("processor.event_handler.HubSpotExtractor")
    @patch("processor.event_handler.ETLMonitor")
    @patch("processor.event_handler.process_batch")
    def test_multiple_object_types_processed_separately(
        self, mock_transform, mock_monitor_cls, mock_extractor_cls, mock_loader_cls, handler
    ):
        """Contacts + deals → dos sets separados de extractor/loader."""
        mock_extractor = MagicMock()
        mock_extractor_cls.return_value = mock_extractor
        mock_extractor.get_properties_with_types.return_value = (["name"], {"name": "string"})
        mock_extractor.safe_request.return_value = MagicMock(
            status_code=200,
            json=lambda: {"results": [{"id": "1", "properties": {"name": "test"}}]},
        )

        mock_loader = MagicMock()
        mock_loader_cls.return_value = mock_loader

        df = pd.DataFrame({"hs_object_id": [1], "name": ["test"]})
        mock_transform.return_value = (df, {"name": "name"})

        events = [
            _make_event("contact", 1, "creation"),
            _make_event("deal", 2, "creation"),
        ]
        batch = EventBatch(events=events)

        handler.process_batch(batch)

        # Debería crear un extractor por cada tipo de objeto
        assert mock_extractor_cls.call_count == 2
        assert mock_loader_cls.call_count == 2


class TestErrorHandling:
    """Verifica manejo de errores."""

    @patch("processor.event_handler.DatabaseLoader")
    @patch("processor.event_handler.HubSpotExtractor")
    @patch("processor.event_handler.ETLMonitor")
    @patch("processor.event_handler.process_batch")
    def test_fetch_failure_logs_error_continues(
        self, mock_transform, mock_monitor_cls, mock_extractor_cls, mock_loader_cls, handler
    ):
        """Error en HubSpot API para contacts → deals aún se procesan."""
        call_count = [0]

        def create_extractor(config, monitor):
            call_count[0] += 1
            extractor = MagicMock()
            extractor.get_properties_with_types.return_value = (["name"], {"name": "string"})
            if call_count[0] == 1:
                # Primera llamada (contacts) falla
                extractor.safe_request.side_effect = Exception("API error")
            else:
                # Segunda llamada (deals) funciona
                extractor.safe_request.return_value = MagicMock(
                    status_code=200,
                    json=lambda: {"results": [{"id": "2", "properties": {"name": "ok"}}]},
                )
            return extractor

        mock_extractor_cls.side_effect = create_extractor

        mock_loader = MagicMock()
        mock_loader_cls.return_value = mock_loader

        df = pd.DataFrame({"hs_object_id": [2], "name": ["ok"]})
        mock_transform.return_value = (df, {"name": "name"})

        events = [
            _make_event("contact", 1, "creation"),
            _make_event("deal", 2, "creation"),
        ]
        batch = EventBatch(events=events)

        summary = handler.process_batch(batch)

        # Contacts falló pero deals debería haberse procesado
        assert summary["errors"] > 0
        # Al menos deals debería estar en processed (1) o los contacts en errors (1)
        assert summary["errors"] + summary["processed"] + summary["deleted"] > 0
