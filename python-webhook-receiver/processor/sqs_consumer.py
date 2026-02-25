"""Consumidor SQS para el event processor."""
import json
import logging

import boto3

from processor.batcher import WebhookEvent

logger = logging.getLogger(__name__)


class SQSConsumer:
    """Consume mensajes de SQS con long-polling."""

    def __init__(self, queue_url: str, region: str = "us-west-2", wait_seconds: int = 20):
        self.queue_url = queue_url
        # Use LocalStack if queue URL points to localhost or localstack container
        if "localhost" in queue_url:
            endpoint_url = "http://localhost:4566"
        elif "localstack" in queue_url:
            endpoint_url = "http://localstack:4566"
        else:
            endpoint_url = None
        self.client = boto3.client("sqs", region_name=region, endpoint_url=endpoint_url)
        self.wait_seconds = wait_seconds

    def poll(self, max_messages: int = 10) -> list[tuple[WebhookEvent, str]]:
        """
        Long-poll SQS para mensajes.

        Returns:
            Lista de tuplas (WebhookEvent, receipt_handle).
        """
        response = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=min(max_messages, 10),
            WaitTimeSeconds=self.wait_seconds,
        )

        messages = response.get("Messages", [])
        results = []

        for msg in messages:
            try:
                body = json.loads(msg["Body"])
                event_data = body.get("event", body)
                event = WebhookEvent.from_hubspot_payload(event_data)
                results.append((event, msg["ReceiptHandle"]))
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.warning(
                    "Mensaje SQS malformado %s: %s",
                    msg.get("MessageId", "?"), e,
                )

        return results

    def acknowledge(self, receipt_handles: list[str]) -> None:
        """Elimina mensajes procesados de SQS."""
        if not receipt_handles:
            return

        for i in range(0, len(receipt_handles), 10):
            chunk = receipt_handles[i:i + 10]
            entries = [
                {"Id": str(idx), "ReceiptHandle": handle}
                for idx, handle in enumerate(chunk)
            ]
            response = self.client.delete_message_batch(
                QueueUrl=self.queue_url,
                Entries=entries,
            )
            failed = response.get("Failed", [])
            if failed:
                logger.warning(
                    "Falló eliminación de %d mensajes SQS: %s",
                    len(failed), failed,
                )
