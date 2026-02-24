"""Configuración centralizada del webhook receiver."""
import os
import sys

from pydantic_settings import BaseSettings

# Agregar python-microservice al path para importar etl/
# IMPORTANTE: usar append (no insert) para no sobreescribir la prioridad
# del directorio raíz del webhook-receiver (evita conflicto con api/)
_microservice_dir = os.path.join(os.path.dirname(__file__), "..", "..", "python-microservice")
_microservice_abs = os.path.abspath(_microservice_dir)
if _microservice_abs not in sys.path:
    sys.path.append(_microservice_abs)


class WebhookConfig(BaseSettings):
    """Configuración cargada desde variables de entorno."""

    # HubSpot
    hubspot_client_secret: str
    hubspot_access_token: str
    webhook_url: str  # URL pública del endpoint (para validación de firma)

    # AWS SQS
    sqs_queue_url: str
    sqs_region: str = "us-west-2"

    # Database (para el event processor, reutiliza credenciales del ETL)
    db_host: str
    db_port: str = "5432"
    db_name: str
    db_user: str
    db_pass: str
    db_schema: str = "hubspot_etl"

    # Event processor
    batch_max_events: int = 100
    batch_wait_seconds: int = 60
    poll_wait_seconds: int = 20

    # Logging
    log_level: str = "INFO"

    # Testing (disable signature validation for local testing)
    skip_signature_validation: bool = False

    model_config = {"env_file": ".env.webhook", "env_file_encoding": "utf-8"}

    def build_etl_config(self, object_type: str):
        """Construye un ETLConfig para un tipo de objeto específico."""
        from etl.config import ETLConfig

        return ETLConfig(
            object_type=object_type,
            access_token=self.hubspot_access_token,
            db_host=self.db_host,
            db_port=self.db_port,
            db_name=self.db_name,
            db_user=self.db_user,
            db_pass=self.db_pass,
            db_schema=self.db_schema,
        )
