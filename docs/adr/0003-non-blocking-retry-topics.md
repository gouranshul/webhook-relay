# ADR 0003: Non-blocking retries with Kafka retry topics, per-message jitter and a DLT

- Status: accepted
- Date: 2026-10-09

## Context

A receiver that is down for ten minutes must not stop deliveries to every other receiver, and it must not be
hammered either. Retrying in place (sleep, try again) on the consumer thread blocks the whole partition, and
with it every endpoint that hashes to that partition. Retries also need to survive a dispatcher restart.

## Decision

Use Spring for Apache Kafka's `@RetryableTopic`:

```
webhook.deliveries ──fail──▶ -retry-0 (~1s) ──▶ -retry-1 (~5s) ──▶ -retry-2 (~25s) ──▶ -retry-3 (~2m) ──▶ -retry-4 (10m) ──▶ -dlt
        │ permanent (4xx except 408/429, redirect, SSRF-blocked URL, unreadable record) ─────────────────────────────────────▶ -dlt
```

- A failed record is re-published to the next retry topic and the original offset is committed, so the main
  partition keeps flowing. A retry-topic consumer pauses only the partition whose head is not yet due.
- `attempts`, initial delay, multiplier and max delay are properties (`relay.retry.*`). The defaults (1 s × 5ⁿ,
  capped at 10 min, 6 attempts) give 1 s, 5 s, 25 s, 125 s and 10 min. That is the closest exponential series to
  the requested 1 s / 5 s / 30 s / 2 m / 10 m; a hand-written list would not be exponential.
- `PermanentDeliveryException` is excluded from retries and goes straight to the DLT.
- The `@DltHandler` marks the delivery `FAILED`, sets `dlt_at` exactly once, and feeds endpoint auto-pause.
  `DltStrategy.ALWAYS_RETRY_ON_ERROR` keeps a DLT record from being dropped if that bookkeeping fails.
- Errors from our own infrastructure (`DataAccessException`) use **blocking** retries (exponential, up to
  2 min) instead. A Postgres blip says nothing about the receiver and should not burn its attempts.

### Jitter has to be per message, and the framework's is not

Spring Kafka 4.x builds each retry topic's delay **once, at startup**. `@BackOff(jitter = ...)` randomises those
per-topic values, but every record in `-retry-2` still waits exactly the same time. If 50,000 deliveries fail
together during a receiver outage, they all come back together, which is a classic retry storm that can knock
the recovering receiver over again.

So `RetryScheduling` adds a DLPR headers function that, for each forwarded record, appends a
`retry_topic-backoff-timestamp` of `now + delay × (1 + U[0, 0.2))`. The retry consumer uses the **last**
such header, so records spread over a 20 % window. The same hook honours `Retry-After`: the due time becomes
`max(jittered delay, Retry-After)`, with Retry-After capped at 1 h (`relay.retry.max-retry-after`) so a hostile
receiver cannot park messages for days. `RetrySchedulingTest` checks the bounds and the spread, and
`DeliveryFlowTest.honoursRetryAfter` checks it end to end (a 2 s `Retry-After` overrides a 300 ms delay).

## Consequences

- Ordering: retries leave the main topic, so a retried delivery can overtake later ones
  ([ADR 0002](0002-at-least-once-and-receiver-dedupe.md)).
- Local deferrals (circuit open, bulkhead full, rate limited) also travel through the retry topics and
  consume an attempt; see [ADR 0004](0004-per-endpoint-isolation.md).
- Each retry topic has its own listener container (`concurrency` consumers each), plus the DLT. With
  virtual threads that costs little memory, but the partition count multiplies across 7 topics.
- Records due later than the topic delay (jitter, Retry-After) pause their partition until due; records
  behind them on the same retry partition wait too. That is acceptable because every record on a retry topic
  is roughly the same age, and the main topic is never affected.
