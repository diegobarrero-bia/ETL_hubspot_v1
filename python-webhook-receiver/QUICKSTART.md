# Quick Start: Testing with Real HubSpot Webhooks

## 5-Minute Setup

### 1. Get Your HubSpot Credentials

1. Go to https://app.hubspot.com/
2. **Settings** → **Integrations** → **Private Apps**
3. Create a new app or select existing one
4. Note these values:
   - **App ID** (visible in the app details)
   - **Access Token** (click "Show token")
   - **Client Secret** (in the "Auth" tab)

Grant these scopes if creating new app:
- `crm.objects.contacts.read`
- `crm.objects.contacts.write`
- `crm.objects.companies.read`
- `crm.objects.deals.read`

### 2. Configure .env.webhook

```bash
cd python-webhook-receiver
cp .env.webhook.example .env.webhook

# Edit .env.webhook with your values:
# HUBSPOT_APP_ID=12345678
# HUBSPOT_CLIENT_SECRET=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
# HUBSPOT_ACCESS_TOKEN=pat-na1-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
# WEBHOOK_URL=https://YOUR-NGROK-URL.ngrok.io/webhooks/hubspot  # Update in step 4
# SQS_QUEUE_URL=http://localhost:4566/000000000000/hubspot-webhooks
# DB_HOST=localhost
# DB_NAME=your_database
# DB_USER=your_user
# DB_PASS=your_password
```

### 3. Start LocalStack (Local SQS)

In terminal 1:

```bash
# Install if needed
pip install localstack

# Start LocalStack
localstack start

# Create queue (in another terminal)
aws --endpoint-url=http://localhost:4566 sqs create-queue \
  --queue-name hubspot-webhooks \
  --region us-west-2
```

### 4. Start ngrok

In terminal 2:

```bash
ngrok http 8080
```

**Important:** Copy the HTTPS URL (e.g., `https://abc123.ngrok.io`) and update `WEBHOOK_URL` in `.env.webhook`:

```bash
WEBHOOK_URL=https://abc123.ngrok.io/webhooks/hubspot
```

### 5. Start Webhook Receiver

In terminal 3:

```bash
cd python-webhook-receiver
source .venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8080
```

Wait for: `INFO: Webhook receiver iniciado`

### 6. Start Event Processor

In terminal 4:

```bash
cd python-webhook-receiver
source .venv/bin/activate
python -m processor
```

Wait for: `INFO: Worker iniciado — esperando eventos de SQS`

### 7. Register Webhooks

In terminal 5:

```bash
cd python-webhook-receiver

# Install requests if needed
pip install requests python-dotenv

# Register webhooks for contacts
python ../scripts/register_webhook.py --object-type contact

# Optional: Register for other object types
python ../scripts/register_webhook.py --object-type deal
python ../scripts/register_webhook.py --object-type company
```

You should see:
```
✓ Registered: contact.creation (id: 123456)
✓ Registered: contact.propertyChange (id: 123457)
✓ Registered: contact.deletion (id: 123458)
✓ Registered: contact.associationChange (id: 123459)
```

### 8. Test It!

Go to your HubSpot account and:

**Test 1: Create a Contact**
1. Go to **Contacts** → **Create contact**
2. Enter name and email
3. Click **Create**

**Watch the logs:**
- Terminal 3 (receiver): `INFO: Eventos enviados a SQS: 1`
- Terminal 4 (processor): `INFO: Batch procesado: 1 processed, 0 deleted, 0 errors`

**Test 2: Update a Contact**
1. Open any contact
2. Change the email or name
3. Click **Save**

**Test 3: Associate Contact with Company**
1. Open a contact
2. Click **Associate** → **Company**
3. Select or create a company

**Test 4: Delete a Contact**
1. Select a contact
2. Click **Delete**

Each action should trigger a webhook!

### 9. Verify in Database

```bash
psql -h localhost -U your_user -d your_database

-- Check if contact was synced
SELECT id, firstname, lastname, email, fivetran_synced
FROM hubspot_etl.contact
ORDER BY fivetran_synced DESC
LIMIT 5;

-- Check soft-deleted contacts
SELECT id, firstname, lastname, fivetran_deleted
FROM hubspot_etl.contact
WHERE fivetran_deleted = true;
```

---

## Troubleshooting

### "401 INVALID_SIGNATURE"

- Check `HUBSPOT_CLIENT_SECRET` is correct
- Verify `WEBHOOK_URL` matches your ngrok URL exactly
- Make sure ngrok is still running (free tier expires after 8 hours)

### "Connection refused to SQS"

- Check LocalStack is running: `curl http://localhost:4566/_localstack/health`
- Recreate the queue if needed (see step 3)

### No Events Arriving

```bash
# List registered webhooks
python ../scripts/register_webhook.py --list

# Check ngrok requests
# Visit: http://localhost:4040 (ngrok web interface)

# Enable debug logging
# In terminal 3, restart with:
LOG_LEVEL=DEBUG uvicorn main:app --host 0.0.0.0 --port 8080
```

### Clean Up Webhooks

```bash
# List all webhooks
python ../scripts/register_webhook.py --list

# Delete all webhooks
python ../scripts/register_webhook.py --delete-all
```

---

## What's Happening Behind the Scenes?

1. **HubSpot** detects a change (create/update/delete/associate)
2. **HubSpot** sends HTTP POST to your ngrok URL
3. **ngrok** forwards to `localhost:8080`
4. **Webhook Receiver** validates HMAC signature and sends to SQS
5. **LocalStack SQS** stores the event
6. **Event Processor** polls SQS, batches events, fetches full data from HubSpot
7. **Event Processor** upserts to PostgreSQL via ETL modules

```
HubSpot → ngrok → Receiver → LocalStack SQS → Processor → PostgreSQL
           (8080)                (4566)
```

---

## Next Steps

- Read [TESTING_GUIDE.md](TESTING_GUIDE.md) for advanced scenarios
- Test with AWS SQS instead of LocalStack
- Set up Docker Compose for easier deployment
- Monitor webhook events in PostgreSQL audit table (coming soon)
