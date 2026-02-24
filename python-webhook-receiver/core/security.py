"""Validación de firma HubSpot Webhook v3."""
import base64
import hashlib
import hmac
import time


class SignatureValidationError(Exception):
    """Error cuando la validación de firma HubSpot falla."""
    pass


def validate_hubspot_signature_v3(
    client_secret: str,
    request_method: str,
    request_url: str,
    request_body: str,
    signature_header: str,
    timestamp_header: str,
    max_age_ms: int = 300_000,
) -> bool:
    """
    Valida la firma X-HubSpot-Signature-v3 de un webhook.

    Algoritmo:
    1. Verificar que el timestamp no sea mayor a max_age_ms (5 min por defecto)
    2. Construir mensaje: method + url + body + timestamp
    3. HMAC-SHA256 con client_secret
    4. Base64 encode y comparar (constant-time)

    Args:
        client_secret: App client secret de HubSpot
        request_method: Método HTTP (ej. "POST")
        request_url: URL completa del endpoint
        request_body: Body del request como string
        signature_header: Valor del header X-HubSpot-Signature-v3
        timestamp_header: Valor del header X-HubSpot-Request-Timestamp (ms)
        max_age_ms: Máxima antigüedad permitida en milisegundos

    Returns:
        True si la firma es válida

    Raises:
        SignatureValidationError: Si el timestamp es demasiado antiguo
    """
    if not signature_header:
        return False

    # 1. Verificar frescura del timestamp (protección contra replay attacks)
    current_ms = int(time.time() * 1000)
    try:
        request_ms = int(timestamp_header)
    except (ValueError, TypeError):
        return False

    if (current_ms - request_ms) > max_age_ms:
        raise SignatureValidationError(
            f"Request timestamp demasiado antiguo: "
            f"{current_ms - request_ms}ms > {max_age_ms}ms permitidos"
        )

    # 2. Construir mensaje: method + url + body + timestamp
    message = f"{request_method}{request_url}{request_body}{timestamp_header}"

    # 3. HMAC-SHA256
    hash_result = hmac.new(
        client_secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).digest()

    # 4. Base64 encode y comparación constant-time
    expected_signature = base64.b64encode(hash_result).decode("utf-8")
    return hmac.compare_digest(expected_signature, signature_header)
