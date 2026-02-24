# Association Change Handling (FR-7)

## Overview

The webhook receiver now supports `associationChange` events from HubSpot, enabling real-time synchronization of CRM object associations to PostgreSQL.

## Implementation

### 1. Data Model Updates ([batcher.py](processor/batcher.py))

Added association-specific fields to `WebhookEvent`:

```python
# New fields in WebhookEvent
from_object_id: Optional[int] = None      # Source object in association
to_object_id: Optional[int] = None        # Target object in association
association_type: Optional[str] = None    # e.g., "CONTACT_TO_COMPANY"
association_removed: Optional[bool] = None # True if removed, False if created
```

**Change type classification**:
- `subscriptionType: "contact.associationChange"` → `change_type: "association"`

### 2. Event Processing ([event_handler.py](processor/event_handler.py))

Added `_process_associations()` method:

**Behavior:**
1. **Collect affected object IDs** - From `objectId` field (the "from" side of the association)
2. **Re-fetch complete records** - Batch read from HubSpot API with updated associations
3. **Re-sync associations** - Call `loader.flush_associations()` to update DB tables
4. **Upsert records** - Update main table with latest data

**Why re-fetch?**
Association webhooks don't include the full association graph - just the change event. We must fetch the current state from HubSpot to get the complete, up-to-date association list.

### 3. HubSpot Webhook Payload

Based on [HubSpot's documentation](https://developers.hubspot.com/docs/api-reference/webhooks-webhooks-v3/guide):

```json
{
  "eventId": 2001,
  "subscriptionType": "contact.associationChange",
  "objectId": 123,
  "occurredAt": 1700000000000,
  "fromObjectId": 123,
  "toObjectId": 456,
  "associationType": "CONTACT_TO_COMPANY",
  "associationRemoved": false,
  "isPrimaryAssociation": true,
  "appId": 9999,
  "portalId": 12345678,
  "attemptNumber": 0
}
```

**Key fields:**
- `fromObjectId` - The object where the association starts (same as `objectId`)
- `toObjectId` - The associated object
- `associationType` - Type like `CONTACT_TO_COMPANY`, `DEAL_TO_LINE_ITEM`
- `associationRemoved` - `true` if removed, `false` if created

**Important:** HubSpot fires **two events** for each association change (one for each direction). For example, associating contact A with company B triggers:
1. Event with `objectId: A` (contact perspective)
2. Event with `objectId: B` (company perspective)

### 4. Deduplication

Association events are deduplicated by `objectId` (the source object), keeping the most recent event. This prevents redundant re-syncs when multiple association changes happen to the same object within a batch window.

### 5. Database Integration

Uses existing ETL infrastructure:
- `DatabaseLoader.flush_associations()` - Updates association bridge tables
- Supports all association types configured in `ETLConfig.associations`

## Testing

### Unit Tests

Added 3 new tests in [test_batcher.py](tests/test_batcher.py):

1. **`test_association_change_parsed_correctly`** - Verifies all association fields are extracted
2. **`test_association_removed_true`** - Tests removal events (`associationRemoved: true`)
3. **`test_association_events_grouped_separately`** - Confirms grouping by object_type works

**Test coverage:** 60/60 tests passing ✅

### Manual Testing

To test with real webhooks:

```bash
curl -X POST http://localhost:8080/webhooks/hubspot \
  -H "Content-Type: application/json" \
  -H "X-HubSpot-Signature-v3: fake" \
  -H "X-HubSpot-Request-Timestamp: $(date +%s)000" \
  -d '[{
    "eventId": 1,
    "subscriptionType": "contact.associationChange",
    "objectId": 159368395530,
    "occurredAt": 1700000000000,
    "fromObjectId": 159368395530,
    "toObjectId": 456,
    "associationType": "CONTACT_TO_COMPANY",
    "associationRemoved": false
  }]'
```

## Limitations

Per [HubSpot's documentation](https://developers.hubspot.com/docs/apps/legacy-apps/public-apps/create-generic-webhook-subscriptions):

1. **Custom objects** - `associationChange` not supported for custom objects in developer platform v2025.2+
2. **User-defined labels** - Associations with custom labels not compatible with generic webhooks
3. **Bidirectional events** - Expect double the webhook volume (one event per side of association)

## Configuration

No additional configuration required. Association handling is automatic when:
- `ETLConfig.associations` is configured for the object type
- HubSpot app has `contact.associationChange`, `deal.associationChange`, etc. subscriptions enabled

## Performance

**API call cost:**
- 1 batch read per unique `fromObjectId` in the batch (not per event, due to deduplication)
- Minimal overhead - only fetches records that actually changed

**Example:**
- 10 association events for contact A → 1 API call
- 5 events for contact A + 5 for contact B → 2 API calls

## Sources

- [HubSpot Webhooks API Guide](https://developers.hubspot.com/docs/api-reference/webhooks-webhooks-v3/guide)
- [New Subscription Types Changelog](https://developers.hubspot.com/changelog/new-subscription-types-for-webhooks)
- [Association Change Bugfix](https://developers.hubspot.com/changelog/bugfix-webhook-associationchanges)
- [Generic Webhook Subscriptions](https://developers.hubspot.com/docs/apps/legacy-apps/public-apps/create-generic-webhook-subscriptions)
