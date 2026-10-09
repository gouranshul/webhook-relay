# ADR 0001: Transactional outbox with a polling relay (not CDC)

- Status: accepted
- Date: 2026-10-09

## Context

`POST /v1/events` must never lose an event and never report success for an event that will not be
delivered. Writing to Postgres and publishing to Kafka are two systems with no shared transaction, so
"insert, then publish" loses events when the process dies between the two steps, and "publish, then insert"
delivers events that were never stored.

The usual fix is a transactional outbox: the API writes the event, one delivery row and one outbox row per
subscribed endpoint, and the idempotency key, all in **one** database transaction. Something else moves
outbox rows to Kafka. That leaves the question of how to read the outbox.

## Options

| | Polling relay (chosen) | CDC with Debezium |
|---|---|---|
| Moving parts | The API process itself | Kafka Connect cluster, Debezium connector, logical replication slot |
| Latency | Poll interval (100 ms idle, back-to-back under load) | Tens of ms, push-based |
| Load on Postgres | One indexed query per shard per poll | WAL decoding; very cheap per row |
| Failure modes | Relay down → rows wait; visible as `outbox.pending` / `outbox.lag.seconds` | An unconsumed replication slot retains WAL **without bound** and can fill the disk |
| Ordering | Explicit: per-shard advisory lock + `ORDER BY id` | Commit order from the WAL |
| Operability for a small team | Plain SQL; a test runs it with Testcontainers | Connector config, schema history topic, slot monitoring, upgrades |
| Multiple instances | `FOR UPDATE SKIP LOCKED` + advisory locks | One connector task per slot |

## Decision

Use a **polling relay** inside `relay-api`:

1. Each outbox row has a `shard = hash(endpoint_id) % 16`.
2. A relay worker, per shard and in one transaction: `pg_try_advisory_xact_lock(ns, shard)` (skip the shard if
   another relay holds it), then
   `SELECT ... WHERE shard = ? AND published_at IS NULL ORDER BY id LIMIT n FOR UPDATE SKIP LOCKED`.
3. Send every row to `webhook.deliveries` keyed by `endpoint_id`, **wait for all acks**, then
   `UPDATE outbox SET published_at = now()` and commit.
4. Published rows are deleted after a retention period (1 h) in bounded batches.

`SKIP LOCKED` alone would let two relays publish rows for the same endpoint concurrently, out of order. The
shard advisory lock makes one relay at a time the publisher for any given endpoint across all instances,
while different shards still publish in parallel. The shard count is a code constant, because rows carry
their shard and changing the count while rows are pending would strand them.

## Consequences

- **At-least-once to Kafka.** If the broker acks but the commit fails (or the process dies between the two),
  the rows are published again. The dispatcher dedupes on `delivery_id`
  ([ADR 0002](0002-at-least-once-and-receiver-dedupe.md)).
- Latency floor is the poll interval when idle. Under load workers loop without sleeping.
- Holding a transaction open while waiting for Kafka acks is bounded by `relay.outbox.publish-timeout` (10 s)
  and the producer's `max.block.ms` / `delivery.timeout.ms`. A Kafka outage shows up as rolled-back batches,
  `outbox.publish.failures`, and a growing `outbox.lag.seconds`. Ingestion keeps working because the API only
  needs Postgres.
- Outbox churn creates dead tuples, so the table gets aggressive autovacuum settings and a partial index
  `(shard, id) WHERE published_at IS NULL`.
- Proven by `OutboxRelayConcurrencyTest`: 3 relays, 10,000 events ingested concurrently, no lost records,
  no duplicates and per-endpoint order preserved.

## When to revisit

Move to CDC if a sustained outbox throughput is needed beyond what one indexed poll per shard can serve, or
if the organisation already runs Kafka Connect and monitors replication slots.
