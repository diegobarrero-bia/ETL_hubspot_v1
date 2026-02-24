"""Tests para WebhookEvent, EventBatch y EventBatcher (Increments 3 y 6)."""
from unittest.mock import patch

import pytest

from processor.batcher import WebhookEvent, EventBatch, EventBatcher


# =====================================================================
# Increment 3: Data Models (WebhookEvent, EventBatch)
# =====================================================================

class TestWebhookEventFromPayload:
    """Verifica parsing de eventos HubSpot a WebhookEvent."""

    def test_webhook_event_from_hubspot_payload(self):
        """Parsea payload de HubSpot en WebhookEvent con campos correctos."""
        payload = {
            "eventId": 1001,
            "subscriptionType": "contact.propertyChange",
            "objectId": 51,
            "occurredAt": 1700000000000,
            "propertyName": "email",
            "propertyValue": "nuevo@example.com",
            "attemptNumber": 0,
            "appId": 9999,
            "portalId": 12345678,
        }
        event = WebhookEvent.from_hubspot_payload(payload)

        assert event.event_id == 1001
        assert event.object_id == 51
        assert event.subscription_type == "contact.propertyChange"
        assert event.occurred_at == 1700000000000
        assert event.property_name == "email"
        assert event.property_value == "nuevo@example.com"
        assert event.attempt_number == 0
        assert event.app_id == 9999
        assert event.portal_id == 12345678

    def test_extracts_object_type_from_subscription(self):
        """'contact.creation' → object_type='contact', 'company.deletion' → 'company'."""
        cases = [
            ("contact.creation", "contact"),
            ("contact.propertyChange", "contact"),
            ("contact.associationChange", "contact"),
            ("company.deletion", "company"),
            ("deal.creation", "deal"),
            ("deal.associationChange", "deal"),
            ("ticket.propertyChange", "ticket"),
            ("line_item.creation", "line_item"),
        ]
        for sub_type, expected_obj_type in cases:
            event = WebhookEvent.from_hubspot_payload({
                "subscriptionType": sub_type,
                "objectId": 1,
            })
            assert event.object_type == expected_obj_type, (
                f"subscriptionType='{sub_type}' debería dar object_type='{expected_obj_type}', "
                f"pero dio '{event.object_type}'"
            )

    def test_classifies_change_type(self):
        """creation/deletion/propertyChange clasificados correctamente."""
        cases = [
            ("contact.creation", "creation"),
            ("deal.deletion", "deletion"),
            ("company.propertyChange", "update"),
        ]
        for sub_type, expected_change_type in cases:
            event = WebhookEvent.from_hubspot_payload({
                "subscriptionType": sub_type,
                "objectId": 1,
            })
            assert event.change_type == expected_change_type

    def test_handles_unknown_subscription_type(self):
        """subscriptionType desconocido no causa crash."""
        event = WebhookEvent.from_hubspot_payload({
            "subscriptionType": "unknown_type",
            "objectId": 99,
        })
        assert event.object_type is not None
        assert event.change_type == "unknown"

    def test_handles_missing_fields_gracefully(self):
        """Payload mínimo (solo objectId) no causa crash."""
        event = WebhookEvent.from_hubspot_payload({"objectId": 42})
        assert event.object_id == 42
        assert event.occurred_at == 0
        assert event.property_name is None


class TestEventBatch:
    """Verifica agrupación y deduplicación de EventBatch."""

    def test_event_batch_groups_by_object_type(self):
        """Batch con contacts + deals → 2 grupos separados."""
        events = [
            WebhookEvent.from_hubspot_payload({
                "subscriptionType": "contact.creation", "objectId": 1, "occurredAt": 100,
            }),
            WebhookEvent.from_hubspot_payload({
                "subscriptionType": "deal.creation", "objectId": 2, "occurredAt": 200,
            }),
            WebhookEvent.from_hubspot_payload({
                "subscriptionType": "contact.propertyChange", "objectId": 3, "occurredAt": 300,
            }),
        ]
        batch = EventBatch(events=events)
        groups = batch.group_by_object_type()

        assert "contact" in groups
        assert "deal" in groups
        assert len(groups["contact"]) == 2
        assert len(groups["deal"]) == 1

    def test_event_batch_deduplicates_keeping_latest(self):
        """Dos eventos para mismo object_id → deduplicate mantiene el más reciente."""
        events = [
            WebhookEvent.from_hubspot_payload({
                "subscriptionType": "contact.propertyChange",
                "objectId": 51,
                "occurredAt": 1000,
                "propertyValue": "viejo",
            }),
            WebhookEvent.from_hubspot_payload({
                "subscriptionType": "contact.propertyChange",
                "objectId": 51,
                "occurredAt": 2000,
                "propertyValue": "nuevo",
            }),
            WebhookEvent.from_hubspot_payload({
                "subscriptionType": "contact.creation",
                "objectId": 52,
                "occurredAt": 1500,
            }),
        ]
        batch = EventBatch(events=events)
        deduped = batch.deduplicate()

        assert len(deduped.events) == 2
        # El evento de object_id=51 debe ser el más reciente
        event_51 = [e for e in deduped.events if e.object_id == 51][0]
        assert event_51.occurred_at == 2000
        assert event_51.property_value == "nuevo"

    def test_deduplicate_preserves_different_object_types_same_id(self):
        """contact 123 and deal 123 must both survive deduplication."""
        events = [
            WebhookEvent.from_hubspot_payload({
                "subscriptionType": "contact.propertyChange",
                "objectId": 123,
                "occurredAt": 1000,
            }),
            WebhookEvent.from_hubspot_payload({
                "subscriptionType": "deal.propertyChange",
                "objectId": 123,
                "occurredAt": 2000,
            }),
        ]
        batch = EventBatch(events=events)
        deduped = batch.deduplicate()

        assert len(deduped.events) == 2
        types = {e.object_type for e in deduped.events}
        assert types == {"contact", "deal"}

    def test_empty_batch_operations(self):
        """Batch vacío no causa errores."""
        batch = EventBatch(events=[])
        assert batch.group_by_object_type() == {}
        deduped = batch.deduplicate()
        assert len(deduped.events) == 0


# =====================================================================
# Increment 6: EventBatcher (batching + deduplication logic)
# =====================================================================

def _make_event(object_id: int, occurred_at: int = 1000) -> WebhookEvent:
    """Helper: crea un WebhookEvent rápido."""
    return WebhookEvent.from_hubspot_payload({
        "subscriptionType": "contact.propertyChange",
        "objectId": object_id,
        "occurredAt": occurred_at,
    })


class TestEventBatcher:
    """Verifica lógica de acumulación, ventana de tiempo y flush."""

    def test_batcher_accumulates_events(self):
        """Agregar 3 eventos → pending_count == 3, is_ready() == False."""
        batcher = EventBatcher(max_batch_size=100, batch_wait_seconds=60)

        for i in range(3):
            batcher.add(_make_event(i))

        assert batcher.pending_count == 3
        assert batcher.is_ready() is False

    def test_batcher_ready_on_max_size(self):
        """Agregar max_batch_size eventos → is_ready() == True inmediatamente."""
        batcher = EventBatcher(max_batch_size=5, batch_wait_seconds=60)

        for i in range(5):
            batcher.add(_make_event(i))

        assert batcher.is_ready() is True

    def test_batcher_ready_after_time_window(self):
        """Avanzar el tiempo más allá de batch_wait_seconds → is_ready() == True."""
        batcher = EventBatcher(max_batch_size=100, batch_wait_seconds=30)
        batcher.add(_make_event(1))

        # Antes de que pase la ventana
        assert batcher.is_ready() is False

        # Simular que pasaron 31 segundos
        with patch("processor.batcher.time") as mock_time:
            mock_time.monotonic.return_value = batcher._window_start + 31
            assert batcher.is_ready() is True

    def test_flush_returns_deduplicated_batch(self):
        """5 eventos con 2 compartiendo object_id → flush retorna 4 eventos."""
        batcher = EventBatcher(max_batch_size=100, batch_wait_seconds=60)

        batcher.add(_make_event(1, occurred_at=100))
        batcher.add(_make_event(2, occurred_at=200))
        batcher.add(_make_event(1, occurred_at=300))  # Duplicado de object_id=1
        batcher.add(_make_event(3, occurred_at=400))
        batcher.add(_make_event(4, occurred_at=500))

        batch = batcher.flush()

        assert len(batch.events) == 4
        # El evento de object_id=1 debe ser el más reciente (occurred_at=300)
        event_1 = [e for e in batch.events if e.object_id == 1][0]
        assert event_1.occurred_at == 300

    def test_flush_resets_state(self):
        """Después de flush, pending_count == 0 y is_ready() == False."""
        batcher = EventBatcher(max_batch_size=100, batch_wait_seconds=60)

        batcher.add(_make_event(1))
        batcher.add(_make_event(2))
        batcher.flush()

        assert batcher.pending_count == 0
        assert batcher.is_ready() is False

    def test_empty_batcher_not_ready(self):
        """Batcher sin eventos → is_ready() == False."""
        batcher = EventBatcher(max_batch_size=100, batch_wait_seconds=60)
        assert batcher.is_ready() is False
        assert batcher.pending_count == 0


class TestAssociationChangeEvents:
    """Tests for association change event parsing (FR-7)."""

    def test_association_change_parsed_correctly(self):
        """Association change payload is parsed with all fields."""
        payload = {
            "eventId": 2001,
            "subscriptionType": "contact.associationChange",
            "objectId": 123,
            "occurredAt": 1700000000000,
            "fromObjectId": 123,
            "toObjectId": 456,
            "associationType": "CONTACT_TO_COMPANY",
            "associationRemoved": False,
            "appId": 9999,
            "portalId": 12345678,
        }
        event = WebhookEvent.from_hubspot_payload(payload)

        assert event.subscription_type == "contact.associationChange"
        assert event.change_type == "association"
        assert event.object_type == "contact"
        assert event.object_id == 123
        assert event.from_object_id == 123
        assert event.to_object_id == 456
        assert event.association_type == "CONTACT_TO_COMPANY"
        assert event.association_removed is False

    def test_association_removed_true(self):
        """Association removal event has associationRemoved=True."""
        payload = {
            "eventId": 2002,
            "subscriptionType": "deal.associationChange",
            "objectId": 789,
            "occurredAt": 1700000001000,
            "fromObjectId": 789,
            "toObjectId": 999,
            "associationType": "DEAL_TO_CONTACT",
            "associationRemoved": True,
        }
        event = WebhookEvent.from_hubspot_payload(payload)

        assert event.change_type == "association"
        assert event.association_removed is True

    def test_association_events_grouped_separately(self):
        """Association events are grouped by object_type like other events."""
        events = [
            WebhookEvent.from_hubspot_payload({
                "subscriptionType": "contact.associationChange",
                "objectId": 1,
                "occurredAt": 100,
            }),
            WebhookEvent.from_hubspot_payload({
                "subscriptionType": "contact.propertyChange",
                "objectId": 2,
                "occurredAt": 200,
            }),
            WebhookEvent.from_hubspot_payload({
                "subscriptionType": "deal.associationChange",
                "objectId": 3,
                "occurredAt": 300,
            }),
        ]
        batch = EventBatch(events=events)
        groups = batch.group_by_object_type()

        assert len(groups["contact"]) == 2
        assert len(groups["deal"]) == 1
