# ADR 0004: Per-endpoint isolation and concurrency: breaker, bulkhead, token bucket, detached attempts

- Status: accepted
- Date: 2026-10-09

## Context

All endpoints share Kafka partitions, and records are keyed by endpoint so first attempts stay in order. A
naive "consume, call the endpoint, wait, commit" loop has two problems:

1. **Noisy neighbours.** A receiver that takes 10 s per request stalls every endpoint on the same partition.
2. **Per-endpoint throughput ceiling.** All of an endpoint's records are on one partition, so a consumer that
   waits for each call delivers at most `1 / latency` per endpoint: about 7/s against a 150 ms receiver,
   however many partitions or pods there are. Real webhook targets need hundreds per second.

A receiver that is down should also not be called thousands of times a minute, and a receiver should not
get more traffic than it asked for.

## Decision

### Per-endpoint guards

Created on first use and held in a size-capped, idle-expiring Caffeine cache (not a Resilience4j registry,
which never forgets):

1. **Circuit breaker** (Resilience4j, count-based window of 20, opens at 50 % failures or 80 % slow calls
   over 5 s, 30 s open). 5xx, 408, 429, timeouts and connection errors count as failures. 2xx **and** 4xx count
   as success, because a 4xx means the receiver is up. While open, the record goes **straight to the next retry
   topic** without calling the endpoint.
2. **Bulkhead**: a semaphore of 50 concurrent attempts per endpoint, with no waiting. When full, the record is
   deferred to the next retry topic. This is both the isolation boundary and the cap on a hot endpoint's
   concurrency.
3. **Rate limit**: a Redis token bucket in one atomic Lua script, shared by all dispatcher instances, using the
   Redis clock. Capacity is one second of rate. Fails open if Redis is down (counted in
   `webhook.ratelimit.errors`).
4. **Global in-flight cap** (2,000 attempts) bounds memory. It is the only thing the consumer thread ever
   waits on: when the whole dispatcher is saturated, waiting is the right back-pressure (lag grows, KEDA
   scales out).

### Detached attempts

```
consumer thread:  cache lookup → breaker → bulkhead → in-flight permit → claim + lease (1 UPDATE) → start attempt → next record
                                                                                                    │
attempt (virtual thread):  token (≤ 5 s) → HTTP (≤ 10 s) → success: record SUCCEEDED
                                                         → failure: forward record to next retry topic / DLT, then record
```

The consumer never waits for HTTP. Many attempts for the same endpoint run at once (up to its bulkhead), so
the per-endpoint ceiling becomes `bulkhead / latency` (about 330/s at 150 ms), and a slow endpoint only ties up
its own permits.

Because the Kafka record is committed before the attempt finishes, the attempt forwards its own failures,
using `RetryForwarder`. That is the same `DeadLetterPublishingRecoverer` that `@RetryableTopic` uses: same
destination resolution, retry headers and jitter/Retry-After function. The **delivery row is the durable
state**. It stays `IN_FLIGHT` under a lease (token wait + request timeout + margin) until the outcome is recorded,
and it only leaves `IN_FLIGHT` for `RETRYING` once its retry record is acked by Kafka. If the dispatcher crashes,
or Kafka is unavailable when the attempt tries to forward, `LeaseSweeper` finds the expired lease and
re-enqueues the delivery through the outbox (`FOR UPDATE SKIP LOCKED`, safe on every instance). The chaos tests
exercise exactly this path by restarting Kafka mid-load.

## Evidence

`NoisyNeighbourIsolationTest` puts endpoint A (10 s latency, bulkhead 4) and endpoint B (healthy) on the
**same partition** with **one** consumer, and sends 200 B and 20 A deliveries over about 4 s. B's p95 from
publish to receipt must be under 1 s. With detached attempts it measured **p95 = 40 ms** (p50 21 ms, max 57 ms),
and A received exactly 4 requests (its bulkhead). Machine: see `docs/benchmarks.md`.

## Alternatives we built, measured and rejected

1. **Wait for the attempt, with a 500 ms handoff.** The consumer waited up to 500 ms, after which the record
   moved to a retry topic and the attempt finished in the background. The isolation test passed (B p95
   618 / 670 / 769 ms over three runs). But a calibration load run showed the per-endpoint ceiling: 12 endpoints,
   200 events/s × fan-out 4 = 800 deliveries/s offered, and Prometheus showed about **29 successful deliveries/s**
   with 2 % dispatcher CPU (4 consumers × 1 / ~0.14 s). The dispatcher was
   idle, waiting.
2. **Listener returns a `CompletableFuture`** (Spring for Apache Kafka 4.1 async acks). The container pauses
   after every poll until all of that poll's futures complete, so one slow attempt stalls its batch unless it
   is timed out. With a timeout added, the async-failure path (error handler plus seeks) left some healthy
   records unprocessed until a later re-fetch: B's p95 was **18–19 s** even though every B attempt took under
   10 ms once started. We judged debugging framework internals a worse trade than a model whose failure modes
   are explicit (lease + sweeper).

## Trade-offs

- **Ordering**: first attempts for an endpoint *start* in partition order, but run concurrently and can
  *complete* out of order (ADR 0002). Receivers must not rely on arrival order.
- **Deferrals use Kafka retry slots.** Circuit-open, bulkhead-full and rate-limited records move one step along
  the retry chain without an HTTP attempt (not recorded in `delivery_attempts`). An endpoint that is throttled
  or broken for longer than the whole chain (about 13 min by default) ends on the DLT, from where it can be
  bulk-replayed. A dedicated delay topic that does not consume attempts is on the roadmap.
- **Crash window**: an attempt that succeeded at the receiver but was not recorded before a crash is sent
  again after the lease expires (at-least-once, ADR 0002).
- Guards are evicted after 1 h idle or beyond 10,000 endpoints. An evicted breaker forgets its history, which
  is safe. An evicted bulkhead could briefly admit more than 50 concurrent attempts, which is harmless.
- Metrics are aggregated (`webhook.circuitbreaker.state{state}`), not tagged per endpoint, to bound
  cardinality. Per-endpoint health comes from `GET /v1/deliveries?endpointId=`.
