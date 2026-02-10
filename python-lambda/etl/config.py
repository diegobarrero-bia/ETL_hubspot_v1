"""Configuración centralizada del ETL."""
import os
import logging

import pytz

logger = logging.getLogger(__name__)

# Timezone por defecto para conversión de fechas
BOGOTA_TZ = pytz.timezone('America/Bogota')


class ETLConfig:
    """
    Configuración del ETL.

    Se puede construir desde:
    - Variables de entorno (modo local / Lambda con env vars)
    - Evento Lambda (invocación directa o EventBridge)
    """

    def __init__(
        self,
        object_type: str,
        access_token: str,
        db_host: str,
        db_port: str,
        db_name: str,
        db_user: str,
        db_pass: str,
        db_schema: str = "hubspot_etl",
        log_level: str = "INFO",
    ):
        self.object_type = object_type
        self.table_name = object_type  # Convención: tabla = tipo de objeto
        self.access_token = access_token
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_schema = db_schema
        self.log_level = log_level.upper()

        # Headers para la API de HubSpot
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
        }

    # -----------------------------------------------------------------
    # Constructores alternativos
    # -----------------------------------------------------------------

    @classmethod
    def from_env(cls) -> "ETLConfig":
        """Construye la configuración desde variables de entorno (.env o Lambda env vars)."""
        return cls(
            object_type=os.getenv("OBJECT_TYPE", "contacts"),
            access_token=os.getenv("ACCESS_TOKEN", ""),
            db_host=os.getenv("DB_HOST", ""),
            db_port=os.getenv("DB_PORT", "5432"),
            db_name=os.getenv("DB_NAME", ""),
            db_user=os.getenv("DB_USER", ""),
            db_pass=os.getenv("DB_PASS", ""),
            db_schema=os.getenv("DB_SCHEMA", "hubspot_etl"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )

    @classmethod
    def from_lambda_event(cls, event: dict) -> "ETLConfig":
        """
        Construye la configuración combinando el evento Lambda con variables de entorno.

        El evento puede sobrescribir ``object_type`` y ``log_level``.
        Las credenciales siempre se leen de variables de entorno (seguridad).
        """
        return cls(
            object_type=event.get("object_type", os.getenv("OBJECT_TYPE", "contacts")),
            access_token=os.getenv("ACCESS_TOKEN", ""),
            db_host=os.getenv("DB_HOST", ""),
            db_port=os.getenv("DB_PORT", "5432"),
            db_name=os.getenv("DB_NAME", ""),
            db_user=os.getenv("DB_USER", ""),
            db_pass=os.getenv("DB_PASS", ""),
            db_schema=os.getenv("DB_SCHEMA", "hubspot_etl"),
            log_level=event.get("log_level", os.getenv("LOG_LEVEL", "INFO")),
        )

    # -----------------------------------------------------------------
    # Validación
    # -----------------------------------------------------------------

    def validate(self) -> None:
        """Valida que todas las variables críticas estén presentes."""
        required = {
            "access_token": "Token de acceso a la API de HubSpot",
            "db_host": "Host de la base de datos PostgreSQL",
            "db_name": "Nombre de la base de datos",
            "db_user": "Usuario de la base de datos",
            "db_pass": "Contraseña de la base de datos",
        }

        missing = []
        for field, description in required.items():
            value = getattr(self, field, None)
            if not value or (isinstance(value, str) and value.strip() == ""):
                missing.append(f"{field} ({description})")

        if missing:
            error_msg = "Variables de configuración faltantes o vacías:\n"
            for var in missing:
                error_msg += f"   - {var}\n"
            raise EnvironmentError(error_msg)

        # Validar log level
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level not in valid_levels:
            logger.warning("LOG_LEVEL inválido '%s', usando 'INFO'", self.log_level)
            self.log_level = "INFO"
