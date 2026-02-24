"""Tests para validación de firma HubSpot v3 (Increment 2)."""
import base64
import hashlib
import hmac
import time

import pytest

from core.security import validate_hubspot_signature_v3, SignatureValidationError


def _compute_signature(secret: str, method: str, url: str, body: str, timestamp: str) -> str:
    """Helper: calcula firma HMAC-SHA256 como lo hace HubSpot."""
    message = f"{method}{url}{body}{timestamp}"
    hash_result = hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(hash_result).decode("utf-8")


SECRET = "test-secret-abc123"
METHOD = "POST"
URL = "https://example.com/webhooks/hubspot"
BODY = '[{"eventId":1,"subscriptionType":"contact.creation","objectId":123}]'


class TestValidSignature:
    """Verifica que firmas válidas pasan la validación."""

    def test_valid_signature_passes(self):
        """Firma correcta con timestamp reciente → True."""
        timestamp = str(int(time.time() * 1000))
        signature = _compute_signature(SECRET, METHOD, URL, BODY, timestamp)

        result = validate_hubspot_signature_v3(
            client_secret=SECRET,
            request_method=METHOD,
            request_url=URL,
            request_body=BODY,
            signature_header=signature,
            timestamp_header=timestamp,
        )
        assert result is True

    def test_empty_body_handled(self):
        """Body vacío no causa error y produce firma válida."""
        timestamp = str(int(time.time() * 1000))
        empty_body = ""
        signature = _compute_signature(SECRET, METHOD, URL, empty_body, timestamp)

        result = validate_hubspot_signature_v3(
            client_secret=SECRET,
            request_method=METHOD,
            request_url=URL,
            request_body=empty_body,
            signature_header=signature,
            timestamp_header=timestamp,
        )
        assert result is True


class TestInvalidSignature:
    """Verifica que firmas inválidas son rechazadas."""

    def test_invalid_signature_rejected(self):
        """Firma alterada → False."""
        timestamp = str(int(time.time() * 1000))
        signature = _compute_signature(SECRET, METHOD, URL, BODY, timestamp)
        tampered = signature[:-4] + "XXXX"

        result = validate_hubspot_signature_v3(
            client_secret=SECRET,
            request_method=METHOD,
            request_url=URL,
            request_body=BODY,
            signature_header=tampered,
            timestamp_header=timestamp,
        )
        assert result is False

    def test_wrong_secret_rejected(self):
        """Firma con secret incorrecto → False."""
        timestamp = str(int(time.time() * 1000))
        signature = _compute_signature("wrong-secret", METHOD, URL, BODY, timestamp)

        result = validate_hubspot_signature_v3(
            client_secret=SECRET,
            request_method=METHOD,
            request_url=URL,
            request_body=BODY,
            signature_header=signature,
            timestamp_header=timestamp,
        )
        assert result is False

    def test_missing_signature_returns_false(self):
        """Firma None o vacía → False."""
        timestamp = str(int(time.time() * 1000))

        result = validate_hubspot_signature_v3(
            client_secret=SECRET,
            request_method=METHOD,
            request_url=URL,
            request_body=BODY,
            signature_header="",
            timestamp_header=timestamp,
        )
        assert result is False


class TestTimestampValidation:
    """Verifica protección contra replay attacks."""

    def test_expired_timestamp_raises(self):
        """Timestamp > 5 minutos de antigüedad → SignatureValidationError."""
        # Timestamp de hace 6 minutos
        old_timestamp = str(int((time.time() - 360) * 1000))
        signature = _compute_signature(SECRET, METHOD, URL, BODY, old_timestamp)

        with pytest.raises(SignatureValidationError, match="timestamp"):
            validate_hubspot_signature_v3(
                client_secret=SECRET,
                request_method=METHOD,
                request_url=URL,
                request_body=BODY,
                signature_header=signature,
                timestamp_header=old_timestamp,
            )

    def test_recent_timestamp_passes(self):
        """Timestamp de hace 2 minutos → pasa validación."""
        recent_timestamp = str(int((time.time() - 120) * 1000))
        signature = _compute_signature(SECRET, METHOD, URL, BODY, recent_timestamp)

        result = validate_hubspot_signature_v3(
            client_secret=SECRET,
            request_method=METHOD,
            request_url=URL,
            request_body=BODY,
            signature_header=signature,
            timestamp_header=recent_timestamp,
        )
        assert result is True
