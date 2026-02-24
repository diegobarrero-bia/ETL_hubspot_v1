"""Tests para configuración de logging JSON estructurado."""
import json
import logging

import pytest

from core.logging_config import setup_logging


class TestSetupLogging:
    """Verifica que setup_logging produce JSON válido con campos esperados."""

    def test_produces_json_with_expected_fields(self, capsys):
        """Log emitido → JSON con timestamp, level, service, message, name, funcName, lineno."""
        setup_logging("test-service")
        logger = logging.getLogger("test.json")
        logger.info("hello structured")

        captured = capsys.readouterr()
        record = json.loads(captured.err)

        assert record["level"] == "INFO"
        assert record["message"] == "hello structured"
        assert record["service"] == "test-service"
        assert record["name"] == "test.json"
        assert "timestamp" in record
        assert "funcName" in record
        assert "lineno" in record

    def test_custom_service_name(self, capsys):
        """service field refleja el argumento pasado."""
        setup_logging("event-processor")
        logger = logging.getLogger("test.svc")
        logger.info("check service")

        captured = capsys.readouterr()
        record = json.loads(captured.err)

        assert record["service"] == "event-processor"

    def test_log_level_from_env(self, monkeypatch, capsys):
        """LOG_LEVEL=DEBUG → mensajes debug emitidos."""
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        setup_logging("test-service")
        logger = logging.getLogger("test.level")
        logger.debug("debug message")

        captured = capsys.readouterr()
        record = json.loads(captured.err)

        assert record["level"] == "DEBUG"
        assert record["message"] == "debug message"
