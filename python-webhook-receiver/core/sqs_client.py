"""Cliente SQS para envío de eventos webhook."""
import json
import logging
import uuid
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)


class SQSClient:
    """Productor SQS: envía eventos webhook a la cola."""

    def __init__(self, queue_url: str, region: str = "us-west-2"):
        self.queue_url = queue_url
        # Use LocalStack if queue URL contains localhost
        endpoint_url = "http://localhost:4566" if "localhost" in queue_url else None
        self.client = boto3.client("sqs", region_name=region, endpoint_url=endpoint_url)

    def check_health(self) -> bool:
        """Verifica conectividad con la cola SQS."""
        try:
            self.client.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )
            return True
        except Exception:
            return False

    def send_events(self, events: list[dict]) -> int:
        """
        Envía eventos webhook a SQS en batches de máx. 10.

        Returns:
            Cantidad de eventos enviados exitosamente.
        """
        if not events:
            return 0

        received_at = datetime.now(timezone.utc).isoformat()
        total_sent = 0

        for i in range(0, len(events), 10):
            chunk = events[i:i + 10]
            entries = []
            for event in chunk:
                sub_type = event.get("subscriptionType", "unknown")
                object_type = sub_type.split(".")[0] if "." in sub_type else "unknown"

                entries.append({
                    "Id": str(uuid.uuid4()),
                    "MessageBody": json.dumps({
                        "event": event,
                        "received_at": received_at,
                    }),
                    "MessageAttributes": {
                        "eventType": {
                            "DataType": "String",
                            "StringValue": sub_type,
                        },
                        "objectType": {
                            "DataType": "String",
                            "StringValue": object_type,
                        },
                        "objectId": {
                            "DataType": "String",
                            "StringValue": str(event.get("objectId", "")),
                        },
                        "receivedAt": {
                            "DataType": "String",
                            "StringValue": received_at,
                        },
                    },
                })

            response = self.client.send_message_batch(
                QueueUrl=self.queue_url,
                Entries=entries,
            )

            failed = response.get("Failed", [])
            if failed:
                logger.error(
                    "SQS batch send parcialmente fallido: %d/%d",
                    len(failed), len(chunk),
                )

            total_sent += len(chunk) - len(failed)

        return total_sent
