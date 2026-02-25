#!/bin/bash
# LocalStack init script — creates SQS queues on startup.
# Mounted at /etc/localstack/init/ready.d/init-sqs.sh

echo "Creating SQS queues..."

# DLQ first (referenced by main queue's redrive policy)
awslocal sqs create-queue \
  --queue-name hubspot-webhook-events-dlq \
  --attributes '{
    "MessageRetentionPeriod": "1209600"
  }'

DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/hubspot-webhook-events-dlq \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text)

# Main queue with redrive policy
awslocal sqs create-queue \
  --queue-name hubspot-webhook-events \
  --attributes '{
    "VisibilityTimeout": "300",
    "MessageRetentionPeriod": "345600",
    "ReceiveMessageWaitTimeSeconds": "20",
    "RedrivePolicy": "{\"deadLetterTargetArn\": \"'"$DLQ_ARN"'\", \"maxReceiveCount\": 3}"
  }'

echo "SQS queues created:"
awslocal sqs list-queues
