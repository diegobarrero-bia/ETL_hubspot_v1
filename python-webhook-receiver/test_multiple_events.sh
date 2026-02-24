#!/bin/bash
# Test script for sending multiple webhook events

WEBHOOK_URL="http://localhost:8080/webhooks/hubspot"

echo "Sending 5 events (3 unique contacts, 2 duplicates)..."

# Event 1: Contact 159368395530 (first update)
curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -H "X-HubSpot-Signature-v3: fake" \
  -H "X-HubSpot-Request-Timestamp: $(date +%s)000" \
  -d '[{"eventId":1,"subscriptionType":"contact.propertyChange","objectId":159368395530,"propertyName":"email","occurredAt":1700000000000}]' \
  -s -o /dev/null -w "Event 1: HTTP %{http_code}\n"

sleep 0.5

# Event 2: Contact 159375851808 (different contact)
curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -H "X-HubSpot-Signature-v3: fake" \
  -H "X-HubSpot-Request-Timestamp: $(date +%s)000" \
  -d '[{"eventId":2,"subscriptionType":"contact.propertyChange","objectId":159375851808,"propertyName":"firstname","occurredAt":1700000001000}]' \
  -s -o /dev/null -w "Event 2: HTTP %{http_code}\n"

sleep 0.5

# Event 3: Contact 159368395530 (duplicate - should dedupe, keeping this one)
curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -H "X-HubSpot-Signature-v3: fake" \
  -H "X-HubSpot-Request-Timestamp: $(date +%s)000" \
  -d '[{"eventId":3,"subscriptionType":"contact.propertyChange","objectId":159368395530,"propertyName":"lastname","occurredAt":1700000002000}]' \
  -s -o /dev/null -w "Event 3: HTTP %{http_code}\n"

sleep 0.5

# Event 4: Contact 159381897053 (third unique contact)
curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -H "X-HubSpot-Signature-v3: fake" \
  -H "X-HubSpot-Request-Timestamp: $(date +%s)000" \
  -d '[{"eventId":4,"subscriptionType":"contact.propertyChange","objectId":159381897053,"propertyName":"phone","occurredAt":1700000003000}]' \
  -s -o /dev/null -w "Event 4: HTTP %{http_code}\n"

sleep 0.5

# Event 5: Contact 159375851808 (duplicate - should dedupe, keeping this one)
curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -H "X-HubSpot-Signature-v3: fake" \
  -H "X-HubSpot-Request-Timestamp: $(date +%s)000" \
  -d '[{"eventId":5,"subscriptionType":"contact.propertyChange","objectId":159375851808,"propertyName":"email","occurredAt":1700000004000}]' \
  -s -o /dev/null -w "Event 5: HTTP %{http_code}\n"

echo ""
echo "✅ Sent 5 events"
echo "Expected after deduplication: 3 unique contacts (159368395530, 159375851808, 159381897053)"
echo ""
echo "Watch Terminal 2 for batch processing (~60 seconds or when batch_max_events reached)..."
