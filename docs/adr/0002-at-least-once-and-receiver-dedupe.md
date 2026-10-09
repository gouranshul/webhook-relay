# ADR 0002: At-least-once delivery, per-endpoint ordering only for first attempts, receivers dedupe

- Status: accepted
- Date: 2026-10-09

## Context

Webhook consumers ask two questions: will I get every event, and will I get each event once and in order?
Exactly-once delivery over HTTP to a third party is impossible. If the receiver processes the request and the
connection drops before the 200 arrives, the sender cannot tell that apart from "never processed". The
honest options are at-most-once (may lose) or at-least-once (may duplicate).

## Decision

### Guarantee: at-least-once

Every accepted event reaches a terminal state for every subscribed endpoint: `SUCCEEDED`, `FAILED` (on the
DLT, replayable) or `CANCELLED` (endpoint deleted). Nothing is dropped silently.

Duplicates are possible and expected in these cases:

| Where | Cause |
|---|---|
| Outbox → Kafka | Broker acked, then the relay's commit failed or the process died. The batch is published again. |
| Kafka → dispatcher | Rebalance or crash before the offset commit. The record is consumed again. |
| Dispatcher → receiver | The receiver processed the request but the response was lost (timeout, reset). Retried. |
| Dispatcher → receiver | The HTTP call succeeded, then the dispatcher died before recording it. The lease expires and another worker sends again. |

### What the relay dedupes itself

The `deliveries` row is the idempotency record. A dispatcher may only send after atomically **claiming** the
row (`PENDING|RETRYING → IN_FLIGHT` with a lease, in one `UPDATE ... RETURNING`). A redelivered Kafka record
for a delivery that is `SUCCEEDED`, `FAILED`, `CANCELLED` or `PARKED`, or `IN_FLIGHT` under a live lease, is
not sent. So the first two rows of the table never reach the receiver, and
`DeliveryFlowTest.redeliveredRecordsNeverCauseASecondSend` shows it: the same record is published three
times and the receiver gets one request. The last two rows cannot be fixed by the sender.

### What receivers must do

Dedupe on `Webhook-Id`. It is stable across retries **and** replays: a replay creates a new delivery row
but copies `webhook_id`. Store processed ids for at least the retry horizon (about 13 minutes with defaults),
and longer if you replay old events.

### Ordering

`webhook.deliveries` is keyed by `endpoint_id`, and the outbox relay publishes each endpoint's rows in
order (ADR 0001), so **first attempts** reach the dispatcher in order per endpoint. Retries break that
order: if event 1 fails and event 2 succeeds, event 2 arrives first and event 1 arrives 1 s to 10 min later.
Handed-off attempts (ADR 0004) can also finish out of order.

Strict per-endpoint ordering would need head-of-line blocking: stop everything for an endpoint until its
oldest event succeeds. One bad event would then stall the endpoint indefinitely, and a slow receiver would
need a dedicated partition. Most webhook consumers cannot rely on order anyway (they also receive events from
other sources), so we choose availability.

Receivers that care about order should use the event's `created_at` or a version number in `data`, and
treat webhooks as "something changed, fetch the current state" signals rather than as an ordered log.

## Consequences

- The README and this ADR say "at-least-once" and "ordered first attempts", never "exactly-once" or "ordered".
- The chaos tests assert **no lost events** (every delivery terminal). They do not assert zero
  duplicates under failure injection, because the at-least-once paths above make duplicates legitimate there.
  They report the duplicate count observed at the receiver instead.
