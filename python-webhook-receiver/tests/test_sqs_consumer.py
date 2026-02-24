"""Tests para SQSConsumer (Increment 7)."""
import json
from unittest.mock import MagicMock, patch

import pytest

from processor.sqs_consumer import SQSConsumer


QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789/webhook-events"


@pytest.fixture
def consumer():
    """SQSConsumer con boto3 mockeado."""
    with patch("processor.sqs_consumer.boto3") as mock_boto3:
        mock_sqs = MagicMock()
        mock_boto3.client.return_value = mock_sqs
        c = SQSConsumer(queue_url=QUEUE_URL, region="us-east-1")
        c._mock_sqs = mock_sqs
        yield c


def _make_sqs_message(event_data: dict, receipt_handle: str = "handle-123") -> dict:
    """Helper: crea un mensaje SQS con formato del webhook receiver."""
    return {
        "MessageId": "msg-001",
        "ReceiptHandle": receipt_handle,
        "Body": json.dumps({
            "event": event_data,
            "received_at": "2026-02-23T10:00:00Z",
        }),
    }


class TestPoll:
    """Verifica polling de mensajes SQS."""

    def test_poll_returns_parsed_events(self, consumer):
        """2 mensajes en cola → retorna 2 tuplas (WebhookEvent, receipt_handle)."""
        messages = [
            _make_sqs_message(
                {"objectId": 1, "subscriptionType": "contact.creation", "occurredAt": 1000},
                "handle-1",
            ),
            _make_sqs_message(
                {"objectId": 2, "subscriptionType": "deal.deletion", "occurredAt": 2000},
                "handle-2",
            ),
        ]
        consumer._mock_sqs.receive_message.return_value = {"Messages": messages}

        results = consumer.poll()

        assert len(results) == 2
        event_1, handle_1 = results[0]
        assert event_1.object_id == 1
        assert event_1.object_type == "contact"
        assert handle_1 == "handle-1"

        event_2, handle_2 = results[1]
        assert event_2.object_id == 2
        assert event_2.change_type == "deletion"
        assert handle_2 == "handle-2"

    def test_poll_empty_queue_returns_empty(self, consumer):
        """Cola vacía → retorna []."""
        consumer._mock_sqs.receive_message.return_value = {}

        results = consumer.poll()

        assert results == []

    def test_poll_uses_long_polling(self, consumer):
        """Verifica que WaitTimeSeconds=20 se usa por defecto."""
        consumer._mock_sqs.receive_message.return_value = {}

        consumer.poll()

        call_kwargs = consumer._mock_sqs.receive_message.call_args.kwargs
        assert call_kwargs["WaitTimeSeconds"] == 20

    def test_malformed_message_skipped_gracefully(self, consumer):
        """1 de 3 mensajes tiene JSON inválido → retorna 2, log warning."""
        messages = [
            _make_sqs_message(
                {"objectId": 1, "subscriptionType": "contact.creation"},
                "handle-1",
            ),
            {
                "MessageId": "msg-bad",
                "ReceiptHandle": "handle-bad",
                "Body": "not valid json {{{",
            },
            _make_sqs_message(
                {"objectId": 3, "subscriptionType": "deal.creation"},
                "handle-3",
            ),
        ]
        consumer._mock_sqs.receive_message.return_value = {"Messages": messages}

        results = consumer.poll()

        assert len(results) == 2


class TestAcknowledge:
    """Verifica eliminación de mensajes procesados."""

    def test_acknowledge_calls_delete_message_batch(self, consumer):
        """3 receipt handles → delete_message_batch llamado correctamente."""
        handles = ["handle-1", "handle-2", "handle-3"]
        consumer._mock_sqs.delete_message_batch.return_value = {
            "Successful": [{"Id": "0"}, {"Id": "1"}, {"Id": "2"}],
            "Failed": [],
        }

        consumer.acknowledge(handles)

        consumer._mock_sqs.delete_message_batch.assert_called_once()
        call_kwargs = consumer._mock_sqs.delete_message_batch.call_args.kwargs
        assert call_kwargs["QueueUrl"] == QUEUE_URL
        assert len(call_kwargs["Entries"]) == 3

    def test_acknowledge_empty_list_does_nothing(self, consumer):
        """Lista vacía → no llama a delete_message_batch."""
        consumer.acknowledge([])

        consumer._mock_sqs.delete_message_batch.assert_not_called()
