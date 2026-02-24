"""Fixtures compartidos para todos los tests del webhook receiver."""
import os
import sys

import pytest

# Agregar el directorio raíz del proyecto webhook-receiver al path
# para poder importar core/, api/, processor/ como módulos
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
if os.path.abspath(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, os.path.abspath(PROJECT_ROOT))

# Agregar el directorio raíz del microservicio existente al path
# para poder importar etl/ modules (config, database, hubspot, etc.)
# IMPORTANTE: usar append (no insert) para que python-webhook-receiver/api/
# tenga prioridad sobre python-microservice/api/
MICROSERVICE_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "python-microservice"
)
if os.path.abspath(MICROSERVICE_DIR) not in sys.path:
    sys.path.append(os.path.abspath(MICROSERVICE_DIR))


@pytest.fixture
def webhook_env(monkeypatch):
    """Variables de entorno mínimas para WebhookConfig."""
    env_vars = {
        "HUBSPOT_CLIENT_SECRET": "test-secret-abc123",
        "HUBSPOT_ACCESS_TOKEN": "pat-fake-token-12345",
        "WEBHOOK_URL": "https://example.com/webhooks/hubspot",
        "SQS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123456789/webhook-events",
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "test_db",
        "DB_USER": "test_user",
        "DB_PASS": "test_pass",
        "DB_SCHEMA": "hubspot_etl",
        "SKIP_SIGNATURE_VALIDATION": "false",
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    return env_vars


@pytest.fixture
def webhook_config(webhook_env):
    """WebhookConfig con valores dummy."""
    from core.config import WebhookConfig
    return WebhookConfig()


@pytest.fixture
def sample_hubspot_webhook_events():
    """Eventos webhook de ejemplo tal como los envía HubSpot."""
    return [
        {
            "eventId": 1001,
            "subscriptionId": 100,
            "portalId": 12345678,
            "appId": 9999,
            "occurredAt": 1700000000000,
            "subscriptionType": "contact.propertyChange",
            "attemptNumber": 0,
            "objectId": 51,
            "propertyName": "email",
            "propertyValue": "nuevo@example.com",
        },
        {
            "eventId": 1002,
            "subscriptionId": 101,
            "portalId": 12345678,
            "appId": 9999,
            "occurredAt": 1700000001000,
            "subscriptionType": "contact.creation",
            "attemptNumber": 0,
            "objectId": 52,
        },
        {
            "eventId": 1003,
            "subscriptionId": 102,
            "portalId": 12345678,
            "appId": 9999,
            "occurredAt": 1700000002000,
            "subscriptionType": "deal.deletion",
            "attemptNumber": 0,
            "objectId": 301,
        },
    ]
