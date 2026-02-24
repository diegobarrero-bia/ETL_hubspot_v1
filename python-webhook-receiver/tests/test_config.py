"""Tests para WebhookConfig (Increment 1)."""
import pytest


class TestWebhookConfigDefaults:
    """Verifica que WebhookConfig carga valores por defecto correctamente."""

    def test_loads_from_env_with_defaults(self, webhook_config):
        """Construye WebhookConfig con env vars mínimas; verificar defaults."""
        assert webhook_config.hubspot_client_secret == "test-secret-abc123"
        assert webhook_config.hubspot_access_token == "pat-fake-token-12345"
        assert webhook_config.webhook_url == "https://example.com/webhooks/hubspot"
        assert webhook_config.sqs_queue_url == "https://sqs.us-east-1.amazonaws.com/123456789/webhook-events"
        # Defaults
        assert webhook_config.sqs_region == "us-west-2"
        assert webhook_config.batch_max_events == 100
        assert webhook_config.batch_wait_seconds == 60
        assert webhook_config.log_level == "INFO"
        assert webhook_config.db_schema == "hubspot_etl"
        assert webhook_config.db_port == "5432"

    def test_custom_batch_settings(self, monkeypatch, webhook_env):
        """Verificar que batch_max_events y batch_wait_seconds se pueden personalizar."""
        monkeypatch.setenv("BATCH_MAX_EVENTS", "200")
        monkeypatch.setenv("BATCH_WAIT_SECONDS", "90")

        from core.config import WebhookConfig
        config = WebhookConfig()

        assert config.batch_max_events == 200
        assert config.batch_wait_seconds == 90


class TestWebhookConfigValidation:
    """Verifica que faltan campos requeridos lanza ValidationError."""

    def test_required_fields_raise_validation_error(self, monkeypatch):
        """Omitir hubspot_client_secret causa ValidationError."""
        # Solo setear algunos campos, omitir los requeridos
        monkeypatch.setenv("WEBHOOK_URL", "https://example.com/webhooks/hubspot")
        # Limpiar variables que podrían existir
        monkeypatch.delenv("HUBSPOT_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("HUBSPOT_ACCESS_TOKEN", raising=False)
        monkeypatch.delenv("SQS_QUEUE_URL", raising=False)
        monkeypatch.delenv("DB_HOST", raising=False)
        monkeypatch.delenv("DB_NAME", raising=False)
        monkeypatch.delenv("DB_USER", raising=False)
        monkeypatch.delenv("DB_PASS", raising=False)
        monkeypatch.delenv("SKIP_SIGNATURE_VALIDATION", raising=False)

        from pydantic import ValidationError
        from core.config import WebhookConfig

        with pytest.raises(ValidationError):
            # _env_file=None evita que lea el .env.webhook del disco
            WebhookConfig(_env_file=None)


class TestWebhookConfigETLBridge:
    """Verifica que WebhookConfig puede construir ETLConfig para el event processor."""

    def test_builds_etl_config_for_object_type(self, webhook_config):
        """build_etl_config() crea ETLConfig con credenciales de DB y object_type."""
        etl_config = webhook_config.build_etl_config("contacts")

        assert etl_config.object_type == "contacts"
        assert etl_config.access_token == "pat-fake-token-12345"
        assert etl_config.db_host == "localhost"
        assert etl_config.db_port == "5432"
        assert etl_config.db_name == "test_db"
        assert etl_config.db_user == "test_user"
        assert etl_config.db_pass == "test_pass"
        assert etl_config.db_schema == "hubspot_etl"

    def test_builds_etl_config_for_different_object_types(self, webhook_config):
        """build_etl_config() funciona para distintos tipos de objetos."""
        for obj_type in ["contacts", "deals", "companies", "tickets", "services"]:
            etl_config = webhook_config.build_etl_config(obj_type)
            assert etl_config.object_type == obj_type
            assert etl_config.table_name == obj_type
