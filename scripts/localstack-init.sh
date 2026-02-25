#!/bin/bash
# LocalStack init script — creates SQS queues on startup.
# Mounted at /etc/localstack/init/ready.d/init-sqs.sh

REGION=us-west-2

echo "Creating SQS queues in $REGION..."

# DLQ first (referenced by main queue's redrive policy)
awslocal sqs create-queue \
  --region $REGION \
  --queue-name hubspot-webhook-events-dlq \
  --attributes '{
    "MessageRetentionPeriod": "1209600"
  }'

DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --region $REGION \
  --queue-url http://sqs.$REGION.localhost.localstack.cloud:4566/000000000000/hubspot-webhook-events-dlq \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text)

echo "DLQ ARN: $DLQ_ARN"

# Main queue with redrive policy
awslocal sqs create-queue \
  --region $REGION \
  --queue-name hubspot-webhook-events \
  --attributes '{
    "VisibilityTimeout": "300",
    "MessageRetentionPeriod": "345600",
    "ReceiveMessageWaitTimeSeconds": "20",
    "RedrivePolicy": "{\"deadLetterTargetArn\": \"'"$DLQ_ARN"'\", \"maxReceiveCount\": 3}"
  }'

echo "SQS queues created:"
awslocal sqs list-queues --region $REGION
