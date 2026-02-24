"""Modelos de datos y lógica de batching para eventos webhook."""
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class WebhookEvent:
    """Evento webhook de HubSpot parseado."""
    event_id: int
    subscription_type: str      # ej. "contact.propertyChange", "contact.associationChange"
    object_id: int
    object_type: str            # ej. "contact" (directo de HubSpot, sin pluralizar)
    change_type: str            # "creation", "deletion", "update", "association", "unknown"
    property_name: Optional[str] = None
    property_value: Optional[str] = None
    occurred_at: int = 0        # timestamp en milisegundos
    attempt_number: int = 0
    app_id: int = 0
    portal_id: int = 0
    # Campos específicos para associationChange
    from_object_id: Optional[int] = None
    to_object_id: Optional[int] = None
    association_type: Optional[str] = None
    association_removed: Optional[bool] = None

    @classmethod
    def from_hubspot_payload(cls, data: dict) -> "WebhookEvent":
        """Parsea un evento del payload webhook de HubSpot."""
        sub_type = data.get("subscriptionType", "")
        parts = sub_type.split(".")
        raw_object = parts[0] if parts else ""
        change_part = parts[1] if len(parts) > 1 else ""

        # Clasificar tipo de cambio
        if "associationChange" in change_part:
            change_type = "association"
        elif "deletion" in change_part:
            change_type = "deletion"
        elif "creation" in change_part:
            change_type = "creation"
        elif "propertyChange" in change_part or "Change" in change_part:
            change_type = "update"
        elif change_part:
            change_type = change_part
        else:
            change_type = "unknown"

        # Usar object type directo de HubSpot (sin pluralizar)
        object_type = raw_object

        return cls(
            event_id=data.get("eventId", 0),
            subscription_type=sub_type,
            object_id=data.get("objectId", 0),
            object_type=object_type,
            change_type=change_type,
            property_name=data.get("propertyName"),
            property_value=data.get("propertyValue"),
            occurred_at=data.get("occurredAt", 0),
            attempt_number=data.get("attemptNumber", 0),
            app_id=data.get("appId", 0),
            portal_id=data.get("portalId", 0),
            # Campos de asociación
            from_object_id=data.get("fromObjectId"),
            to_object_id=data.get("toObjectId"),
            association_type=data.get("associationType"),
            association_removed=data.get("associationRemoved"),
        )


@dataclass
class EventBatch:
    """Batch de eventos deduplicados listos para procesar."""
    events: list[WebhookEvent] = field(default_factory=list)

    def group_by_object_type(self) -> dict[str, list[WebhookEvent]]:
        """Agrupa eventos por object_type."""
        groups: dict[str, list[WebhookEvent]] = defaultdict(list)
        for event in self.events:
            groups[event.object_type].append(event)
        return dict(groups)

    def deduplicate(self) -> "EventBatch":
        """Mantiene solo el evento más reciente por (object_type, object_id)."""
        latest: dict[tuple[str, int], WebhookEvent] = {}
        for event in self.events:
            key = (event.object_type, event.object_id)
            existing = latest.get(key)
            if existing is None or event.occurred_at > existing.occurred_at:
                latest[key] = event
        return EventBatch(events=list(latest.values()))


class EventBatcher:
    """
    Acumula eventos y los deduplicable por object_id.
    Hace flush cuando se alcanza max_batch_size O batch_wait_seconds.
    """

    def __init__(self, max_batch_size: int = 100, batch_wait_seconds: int = 60):
        self.max_batch_size = max_batch_size
        self.batch_wait_seconds = batch_wait_seconds
        self._events: list[WebhookEvent] = []
        self._window_start: float | None = None

    def add(self, event: WebhookEvent) -> None:
        """Agrega un evento al batch pendiente."""
        if self._window_start is None:
            self._window_start = time.monotonic()
        self._events.append(event)

    @property
    def pending_count(self) -> int:
        """Cantidad de eventos pendientes."""
        return len(self._events)

    def is_ready(self) -> bool:
        """Verifica si el batch debe ser procesado (por tamaño o tiempo)."""
        if not self._events:
            return False
        if len(self._events) >= self.max_batch_size:
            return True
        if self._window_start is not None:
            elapsed = time.monotonic() - self._window_start
            return elapsed >= self.batch_wait_seconds
        return False

    def flush(self) -> EventBatch:
        """Retorna el batch actual deduplicado y resetea el estado."""
        batch = EventBatch(events=list(self._events))
        batch = batch.deduplicate()
        self._events.clear()
        self._window_start = None
        return batch
