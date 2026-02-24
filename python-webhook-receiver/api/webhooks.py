"""Endpoint para recibir webhooks de HubSpot."""
import json
import logging

from fastapi import APIRouter, Request, HTTPException

from core.config import WebhookConfig
from core.security import validate_hubspot_signature_v3, SignatureValidationError
from core.sqs_client import SQSClient

logger = logging.getLogger(__name__)

router = APIRouter(tags=["webhooks"])

# Dependencias inyectadas en startup
_config: WebhookConfig | None = None
_sqs_client: SQSClient | None = None


def init_dependencies(config: WebhookConfig):
    """Inicializa dependencias del endpoint. Llamado en startup de la app."""
    global _config, _sqs_client
    _config = config
    _sqs_client = SQSClient(config.sqs_queue_url, config.sqs_region)


def get_queue_health() -> bool | None:
    """Retorna estado de conectividad SQS, o None si el cliente no está inicializado."""
    if _sqs_client is None:
        return None
    return _sqs_client.check_health()


@router.post("/webhooks/hubspot")
async def receive_hubspot_webhook(request: Request):
    """
    Recibe eventos webhook de HubSpot.

    1. Valida firma X-HubSpot-Signature-v3
    2. Envía eventos a SQS
    3. Retorna 200 OK (incluso si SQS falla → graceful degradation)
    """
    # 1. Verificar headers de firma
    signature = request.headers.get("X-HubSpot-Signature-v3")
    timestamp = request.headers.get("X-HubSpot-Request-Timestamp")

    if not signature or not timestamp:
        raise HTTPException(
            status_code=401,
            detail={"error": "MISSING_SIGNATURE_HEADERS"},
        )

    # 2. Leer body crudo (necesario para validación de firma)
    body = await request.body()
    body_str = body.decode("utf-8")

    # 3. Validar firma (skip en modo testing)
    if not _config.skip_signature_validation:
        try:
            is_valid = validate_hubspot_signature_v3(
                client_secret=_config.hubspot_client_secret,
                request_method="POST",
                request_url=_config.webhook_url,
                request_body=body_str,
                signature_header=signature,
                timestamp_header=timestamp,
            )
            if not is_valid:
                logger.warning("Firma inválida recibida")
                raise HTTPException(
                    status_code=401,
                    detail={"error": "INVALID_SIGNATURE"},
                )
        except SignatureValidationError as e:
            logger.warning("Validación de firma falló: %s", e)
            raise HTTPException(
                status_code=401,
                detail={"error": "INVALID_SIGNATURE", "message": str(e)},
            )
    else:
        logger.warning("⚠️  SIGNATURE VALIDATION DISABLED - TESTING MODE")

    # 4. Parsear eventos
    events = json.loads(body_str) if body_str else []

    # 5. Enviar a SQS (graceful degradation: log error pero retornar 200)
    try:
        sent = _sqs_client.send_events(events)
        logger.info("Eventos enviados a SQS: %d", sent)
        return {"received": sent}
    except Exception as e:
        logger.error("Error enviando a SQS: %s", e, exc_info=True)
        return {"received": 0, "sqs_error": str(e)}
