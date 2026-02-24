"""Tests para SQSClient (Producer) — Increment 4."""
import json
from unittest.mock import MagicMock, patch

import pytest

from core.sqs_client import SQSClient


QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789/webhook-events"


@pytest.fixture
def sqs_client():
    """SQSClient con boto3 mockeado."""
    with patch("core.sqs_client.boto3") as mock_boto3:
        mock_sqs = MagicMock()
        mock_boto3.client.return_value = mock_sqs
        mock_sqs.send_message_batch.return_value = {"Successful": [], "Failed": []}
        client = SQSClient(queue_url=QUEUE_URL, region="us-east-1")
        client._mock_sqs = mock_sqs  # para inspección en tests
        yield client


class TestSendEvents:
    """Verifica envío de eventos a SQS."""

    def test_send_events_calls_send_message_batch(self, sqs_client):
        """Enviar 3 eventos → send_message_batch llamado con 3 entries."""
        events = [
            {"objectId": 1, "subscriptionType": "contact.creation"},
            {"objectId": 2, "subscriptionType": "deal.creation"},
            {"objectId": 3, "subscriptionType": "company.creation"},
        ]
        sqs_client._mock_sqs.send_message_batch.return_value = {
            "Successful": [{"Id": "0"}, {"Id": "1"}, {"Id": "2"}],
            "Failed": [],
        }

        sent = sqs_client.send_events(events)

        assert sent == 3
        sqs_client._mock_sqs.send_message_batch.assert_called_once()
        call_kwargs = sqs_client._mock_sqs.send_message_batch.call_args
        assert call_kwargs.kwargs["QueueUrl"] == QUEUE_URL
        entries = call_kwargs.kwargs["Entries"]
        assert len(entries) == 3

    def test_send_events_chunks_at_10(self, sqs_client):
        """Enviar 15 eventos → 2 llamadas a send_message_batch (10 + 5)."""
        events = [{"objectId": i, "subscriptionType": "contact.creation"} for i in range(15)]
        sqs_client._mock_sqs.send_message_batch.return_value = {
            "Successful": [{"Id": str(i)} for i in range(10)],
            "Failed": [],
        }

        sent = sqs_client.send_events(events)

        assert sqs_client._mock_sqs.send_message_batch.call_count == 2
        # Primera llamada: 10 entries
        first_call = sqs_client._mock_sqs.send_message_batch.call_args_list[0]
        assert len(first_call.kwargs["Entries"]) == 10
        # Segunda llamada: 5 entries
        second_call = sqs_client._mock_sqs.send_message_batch.call_args_list[1]
        assert len(second_call.kwargs["Entries"]) == 5

    def test_send_empty_list_returns_zero(self, sqs_client):
        """Lista vacía → retorna 0, sin llamadas a boto3."""
        sent = sqs_client.send_events([])

        assert sent == 0
        sqs_client._mock_sqs.send_message_batch.assert_not_called()

    def test_send_failure_propagates_error(self, sqs_client):
        """Error de boto3 → excepción propagada."""
        sqs_client._mock_sqs.send_message_batch.side_effect = Exception("SQS down")

        with pytest.raises(Exception, match="SQS down"):
            sqs_client.send_events([{"objectId": 1}])

    def test_message_body_includes_received_at(self, sqs_client):
        """El MessageBody incluye received_at con timestamp ISO."""
        events = [{"objectId": 1, "subscriptionType": "contact.creation"}]
        sqs_client._mock_sqs.send_message_batch.return_value = {
            "Successful": [{"Id": "0"}],
            "Failed": [],
        }

        sqs_client.send_events(events)

        call_kwargs = sqs_client._mock_sqs.send_message_batch.call_args
        entries = call_kwargs.kwargs["Entries"]
        body = json.loads(entries[0]["MessageBody"])
        assert "received_at" in body
        assert "event" in body
        assert body["event"]["objectId"] == 1

    def test_reports_partial_failures(self, sqs_client):
        """Si SQS reporta failures parciales → retorna solo los exitosos."""
        events = [{"objectId": i} for i in range(3)]
        sqs_client._mock_sqs.send_message_batch.return_value = {
            "Successful": [{"Id": "0"}, {"Id": "1"}],
            "Failed": [{"Id": "2", "Message": "error"}],
        }

        sent = sqs_client.send_events(events)

        assert sent == 2


class TestCheckHealth:
    """Verifica check_health() de SQSClient."""

    def test_check_health_returns_true_when_queue_accessible(self, sqs_client):
        """get_queue_attributes exitoso → True."""
        sqs_client._mock_sqs.get_queue_attributes.return_value = {
            "Attributes": {"ApproximateNumberOfMessages": "5"}
        }

        assert sqs_client.check_health() is True
        sqs_client._mock_sqs.get_queue_attributes.assert_called_once_with(
            QueueUrl=QUEUE_URL,
            AttributeNames=["ApproximateNumberOfMessages"],
        )

    def test_check_health_returns_false_when_queue_unreachable(self, sqs_client):
        """get_queue_attributes falla → False."""
        sqs_client._mock_sqs.get_queue_attributes.side_effect = Exception("Connection refused")

        assert sqs_client.check_health() is False
