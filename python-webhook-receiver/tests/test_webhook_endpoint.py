"""Tests para el endpoint webhook y health (Increment 5)."""
import base64
import hashlib
import hmac
import json
import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


def _compute_signature(secret: str, method: str, url: str, body: str, timestamp: str) -> str:
    """Helper: calcula firma HMAC-SHA256 v3."""
    message = f"{method}{url}{body}{timestamp}"
    hash_result = hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(hash_result).decode("utf-8")


SECRET = "test-secret-abc123"
WEBHOOK_URL = "https://example.com/webhooks/hubspot"


@pytest.fixture
def client(webhook_config):
    """TestClient de FastAPI con dependencias inyectadas."""
    from api.webhooks import init_dependencies
    from main import app

    # Mock SQS client para evitar llamadas reales a AWS
    with patch("api.webhooks.SQSClient") as MockSQSClient:
        mock_sqs = MagicMock()
        mock_sqs.send_events.return_value = 2
        MockSQSClient.return_value = mock_sqs

        init_dependencies(webhook_config)

        # Reemplazar el _sqs_client con nuestro mock
        import api.webhooks as webhooks_module
        webhooks_module._sqs_client = mock_sqs

        test_client = TestClient(app)
        test_client._mock_sqs = mock_sqs
        yield test_client


def _post_webhook(client, events: list[dict], secret: str = SECRET, url: str = WEBHOOK_URL,
                  timestamp: str = None, signature: str = None):
    """Helper: envía POST /webhooks/hubspot con firma válida."""
    body = json.dumps(events)
    if timestamp is None:
        timestamp = str(int(time.time() * 1000))
    if signature is None:
        signature = _compute_signature(secret, "POST", url, body, timestamp)

    return client.post(
        "/webhooks/hubspot",
        content=body,
        headers={
            "X-HubSpot-Signature-v3": signature,
            "X-HubSpot-Request-Timestamp": timestamp,
            "Content-Type": "application/json",
        },
    )


class TestWebhookEndpoint:
    """Tests para POST /webhooks/hubspot."""

    def test_valid_webhook_returns_200(self, client):
        """Firma válida + 2 eventos → 200 con count correcto."""
        events = [
            {"objectId": 1, "subscriptionType": "contact.creation"},
            {"objectId": 2, "subscriptionType": "deal.creation"},
        ]
        response = _post_webhook(client, events)

        assert response.status_code == 200
        data = response.json()
        assert data["received"] == 2

    def test_invalid_signature_returns_401(self, client):
        """Firma incorrecta → 401."""
        events = [{"objectId": 1, "subscriptionType": "contact.creation"}]
        response = _post_webhook(client, events, signature="INVALID_SIGNATURE_XXX")

        assert response.status_code == 401

    def test_missing_signature_headers_returns_401(self, client):
        """Sin header X-HubSpot-Signature-v3 → 401."""
        response = client.post(
            "/webhooks/hubspot",
            content=json.dumps([{"objectId": 1}]),
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 401

    def test_expired_timestamp_returns_401(self, client):
        """Timestamp > 5 min → 401."""
        events = [{"objectId": 1, "subscriptionType": "contact.creation"}]
        old_timestamp = str(int((time.time() - 600) * 1000))  # 10 min ago
        response = _post_webhook(client, events, timestamp=old_timestamp)

        assert response.status_code == 401

    def test_sqs_failure_still_returns_200(self, client):
        """Error de SQS → aún retorna 200 (graceful degradation)."""
        client._mock_sqs.send_events.side_effect = Exception("SQS down")

        events = [{"objectId": 1, "subscriptionType": "contact.creation"}]
        response = _post_webhook(client, events)

        assert response.status_code == 200
        data = response.json()
        assert "sqs_error" in data
        assert data["received"] == 0

    def test_empty_payload_returns_200(self, client):
        """Payload vacío [] → 200 con received=0."""
        client._mock_sqs.send_events.return_value = 0
        response = _post_webhook(client, [])

        assert response.status_code == 200
        data = response.json()
        assert data["received"] == 0


class TestHealthEndpoint:
    """Tests para GET /health."""

    def test_health_endpoint_returns_200(self, client):
        """GET /health → 200 con status healthy."""
        response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "webhook-receiver"
