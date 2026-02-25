# Testing with Real HubSpot Webhooks

## Prerequisites

1. **HubSpot Developer Account** with a test app
2. **Public URL** for your webhook receiver (ngrok or similar)
3. **AWS SQS Queue** or **LocalStack** for local testing

## Option A: Quick Test with LocalStack (Recommended for First Test)

### 1. Start LocalStack with SQS

```bash
# Install LocalStack (if not already installed)
pip install localstack

# Start LocalStack in a terminal
localstack start

# In another terminal, create an SQS queue
aws --endpoint-url=http://localhost:4566 sqs create-queue \
  --queue-name hubspot-webhooks \
  --region us-west-2
```

The queue URL will be: `http://localhost:4566/000000000000/hubspot-webhooks`

### 2. Expose Local Webhook Receiver via ngrok

```bash
# Install ngrok: https://ngrok.com/download
# Start ngrok on port 8080
ngrok http 8080
```

Copy the **HTTPS URL** from ngrok output (e.g., `https://abc123.ngrok.io`)

### 3. Configure Environment Variables

Edit `.env.webhook`:

```bash
# HubSpot
HUBSPOT_CLIENT_SECRET=your-app-client-secret-from-hubspot
HUBSPOT_ACCESS_TOKEN=your-private-app-access-token
WEBHOOK_URL=https://abc123.ngrok.io/webhooks/hubspot  # Your ngrok URL

# LocalStack SQS
SQS_QUEUE_URL=http://localhost:4566/000000000000/hubspot-webhooks
SQS_REGION=us-west-2

# Database (your existing PostgreSQL)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASS=your_db_password
DB_SCHEMA=hubspot_etl

# For testing, disable signature validation initially
SKIP_SIGNATURE_VALIDATION=false  # Set to true only for initial debugging
```

### 4. Start the Webhook Receiver

```bash
cd python-webhook-receiver
source .venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8080
```

### 5. Start the Event Processor

In another terminal:

```bash
cd python-webhook-receiver
source .venv/bin/activate
python -m processor
```

### 6. Register Webhooks in HubSpot

#### Get Your HubSpot App ID

1. Go to https://app.hubspot.com/
2. Navigate to **Settings** → **Integrations** → **Private Apps** (or Developer Apps)
3. Note your **App ID**

#### Register Webhook Subscription via API

```bash
# Set your values
APP_ID=your_app_id
ACCESS_TOKEN=your_access_token
WEBHOOK_URL=https://abc123.ngrok.io/webhooks/hubspot

# Register contact webhooks
curl -X POST "https://api.hubspot.com/webhooks/v3/${APP_ID}/subscriptions" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "active": true,
    "eventType": "contact.propertyChange",
    "propertyName": "email"
  }'

curl -X POST "https://api.hubspot.com/webhooks/v3/${APP_ID}/subscriptions" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "active": true,
    "eventType": "contact.creation"
  }'

curl -X POST "https://api.hubspot.com/webhooks/v3/${APP_ID}/subscriptions" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "active": true,
    "eventType": "contact.deletion"
  }'

curl -X POST "https://api.hubspot.com/webhooks/v3/${APP_ID}/subscriptions" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "active": true,
    "eventType": "contact.associationChange"
  }'
```

#### Verify Subscriptions

```bash
# List all subscriptions
curl -X GET "https://api.hubspot.com/webhooks/v3/${APP_ID}/subscriptions" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}"
```

### 7. Trigger Test Events in HubSpot

1. **Create a Contact**: Go to HubSpot → Contacts → Create contact
2. **Update a Contact**: Edit an existing contact's email
3. **Associate Objects**: Link a contact to a company
4. **Delete a Contact**: Delete a test contact

### 8. Monitor the Logs

**Webhook Receiver terminal:**
```
INFO: Eventos enviados a SQS: 1
```

**Event Processor terminal:**
```
INFO: Batch procesado: 1 processed, 0 deleted, 0 errors
```

**Check LocalStack SQS:**
```bash
aws --endpoint-url=http://localhost:4566 sqs receive-message \
  --queue-url http://localhost:4566/000000000000/hubspot-webhooks \
  --region us-west-2
```

---

## Option B: Testing with Real AWS SQS

### 1. Create SQS Queue in AWS

```bash
# Using AWS CLI
aws sqs create-queue \
  --queue-name hubspot-webhooks \
  --region us-west-2
```

Note the **QueueUrl** from the output.

### 2. Configure AWS Credentials

```bash
# Set AWS credentials (if not already configured)
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-west-2
```

### 3. Update .env.webhook

```bash
SQS_QUEUE_URL=https://sqs.us-west-2.amazonaws.com/123456789012/hubspot-webhooks
SQS_REGION=us-west-2
```

### 4. Follow Steps 2-8 from Option A

---

## Troubleshooting

### Signature Validation Failing

If you see `401 INVALID_SIGNATURE` errors:

1. Verify `HUBSPOT_CLIENT_SECRET` is correct (from HubSpot app settings)
2. Verify `WEBHOOK_URL` matches exactly what ngrok provides (including `/webhooks/hubspot`)
3. Check that ngrok hasn't expired and generated a new URL

### Events Not Processing

```bash
# Check SQS queue has messages
aws --endpoint-url=http://localhost:4566 sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/hubspot-webhooks \
  --attribute-names ApproximateNumberOfMessages

# Check processor logs for errors
python -m processor --log-level DEBUG
```

### Database Connection Errors

Verify your PostgreSQL is running and credentials are correct:

```bash
psql -h localhost -U your_db_user -d your_db_name -c "SELECT 1;"
```

---

## Advanced: Testing Specific Event Types

### Test Association Changes

```bash
# In HubSpot UI:
# 1. Go to a Contact
# 2. Click "Associate" → "Company"
# 3. Select or create a company
# → Should trigger contact.associationChange webhook
```

### Test Property Changes

```bash
# Create webhook for specific property
curl -X POST "https://api.hubspot.com/webhooks/v3/${APP_ID}/subscriptions" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "active": true,
    "eventType": "contact.propertyChange",
    "propertyName": "firstname"
  }'

# Update contact's firstname in HubSpot UI → triggers webhook
```

---

## Clean Up

### Unregister All Webhooks

```bash
# Get subscription IDs
SUBSCRIPTIONS=$(curl -s -X GET \
  "https://api.hubspot.com/webhooks/v3/${APP_ID}/subscriptions" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}")

echo $SUBSCRIPTIONS | jq -r '.results[].id' | while read SUB_ID; do
  curl -X DELETE \
    "https://api.hubspot.com/webhooks/v3/${APP_ID}/subscriptions/${SUB_ID}" \
    -H "Authorization: Bearer ${ACCESS_TOKEN}"
  echo "Deleted subscription: $SUB_ID"
done
```

### Stop Services

```bash
# Ctrl+C in webhook receiver terminal
# Ctrl+C in event processor terminal
# Ctrl+C in ngrok terminal
# Ctrl+C in LocalStack terminal (or: localstack stop)
```
