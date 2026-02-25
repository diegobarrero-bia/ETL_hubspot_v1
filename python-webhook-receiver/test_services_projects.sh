#!/bin/bash
# Test script for services and projects webhook events
# Uses real HubSpot object IDs

WEBHOOK_URL="http://localhost:8080/webhooks/hubspot"

echo "=== SERVICES EVENTS ==="

# Service 1: creation
curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -H "X-HubSpot-Signature-v3: fake" \
  -H "X-HubSpot-Request-Timestamp: $(date +%s)000" \
  -d '[{"eventId":100,"subscriptionType":"services.creation","objectId":513220065453,"occurredAt":1700000000000}]' \
  -s -o /dev/null -w "Service 513220065453 (creation):  HTTP %{http_code}\n"

sleep 0.5

# Service 2: propertyChange
curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -H "X-HubSpot-Signature-v3: fake" \
  -H "X-HubSpot-Request-Timestamp: $(date +%s)000" \
  -d '[{"eventId":101,"subscriptionType":"services.propertyChange","objectId":513220187805,"propertyName":"name","occurredAt":1700000001000}]' \
  -s -o /dev/null -w "Service 513220187805 (update):    HTTP %{http_code}\n"

sleep 0.5

# Service 3: propertyChange
curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -H "X-HubSpot-Signature-v3: fake" \
  -H "X-HubSpot-Request-Timestamp: $(date +%s)000" \
  -d '[{"eventId":102,"subscriptionType":"services.propertyChange","objectId":513220187806,"propertyName":"status","occurredAt":1700000002000}]' \
  -s -o /dev/null -w "Service 513220187806 (update):    HTTP %{http_code}\n"

sleep 0.5

# Service 1 again (duplicate — should be deduplicated, keeping this newer one)
curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -H "X-HubSpot-Signature-v3: fake" \
  -H "X-HubSpot-Request-Timestamp: $(date +%s)000" \
  -d '[{"eventId":103,"subscriptionType":"services.propertyChange","objectId":513220065453,"propertyName":"price","occurredAt":1700000003000}]' \
  -s -o /dev/null -w "Service 513220065453 (dup):       HTTP %{http_code}\n"

sleep 0.5

echo ""
echo "=== PROJECTS EVENTS ==="

# Project 1: creation
curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -H "X-HubSpot-Signature-v3: fake" \
  -H "X-HubSpot-Request-Timestamp: $(date +%s)000" \
  -d '[{"eventId":200,"subscriptionType":"projects.creation","objectId":528752155539,"occurredAt":1700000004000}]' \
  -s -o /dev/null -w "Project 528752155539 (creation):  HTTP %{http_code}\n"

sleep 0.5

# Project 2: propertyChange
curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -H "X-HubSpot-Signature-v3: fake" \
  -H "X-HubSpot-Request-Timestamp: $(date +%s)000" \
  -d '[{"eventId":201,"subscriptionType":"projects.propertyChange","objectId":528767884309,"propertyName":"name","occurredAt":1700000005000}]' \
  -s -o /dev/null -w "Project 528767884309 (update):    HTTP %{http_code}\n"

sleep 0.5

# Project 3: propertyChange
curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -H "X-HubSpot-Signature-v3: fake" \
  -H "X-HubSpot-Request-Timestamp: $(date +%s)000" \
  -d '[{"eventId":202,"subscriptionType":"projects.propertyChange","objectId":529044624407,"propertyName":"status","occurredAt":1700000006000}]' \
  -s -o /dev/null -w "Project 529044624407 (update):    HTTP %{http_code}\n"

sleep 0.5

# Project 2 again (duplicate)
curl -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -H "X-HubSpot-Signature-v3: fake" \
  -H "X-HubSpot-Request-Timestamp: $(date +%s)000" \
  -d '[{"eventId":203,"subscriptionType":"projects.propertyChange","objectId":528767884309,"propertyName":"deadline","occurredAt":1700000007000}]' \
  -s -o /dev/null -w "Project 528767884309 (dup):       HTTP %{http_code}\n"

echo ""
echo "=== SUMMARY ==="
echo "Sent 8 events total (4 services + 4 projects)"
echo "After deduplication: 3 unique services + 3 unique projects = 6 records"
echo ""
echo "Watch 'docker logs -f event-processor' for batch processing (~60s)..."
