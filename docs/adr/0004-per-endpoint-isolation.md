# ADR 0004: Per-endpoint isolation: circuit breaker, bulkhead, token bucket and handoff

- Status: accepted
- Date: 2026-10-09

## Context

All endpoints share Kafka partitions. A consumer thread processes a partition's records one after another,
so with a naive "call the endpoint and wait" loop, one receiver that takes 10 s per request stalls every
other endpoint hashed to the same partition. That is the noisy-neighbour problem. A receiver that is down
should also not be called thousands of times a minute, and a receiver should not get more traffic than it
asked for.

## Decision

Per endpoint, created on first use and held in a size-capped, idle-expiring Caffeine cache (not a
Resilience4j registry, which never forgets):

1. **Circuit breaker** (Resilience4j, count-based window of 20, opens at 50 % failures or 80 % slow calls
   over 5 s, 30 s open). 5xx, 408, 429, timeouts and connection errors count as failures. 2xx **and** 4xx
   count as success, because a 4xx means the receiver is up. While open, the record goes **straight to the
   next retry topic** without calling the endpoint.
2. **Bulkhead**: a semaphore of 10 concurrent attempts, with no waiting. When full, the record is deferred to
   the next retry topic.
3. **Rate limit**: a Redis token bucket in one atomic Lua script, shared by all dispatcher instances, using the
   Redis clock. Capacity is one second of rate. Fails open if Redis is down (counted in
   `webhook.ratelimit.errors`).
4. **Global in-flight cap** (2,000) bounds the memory of everything running at once.

And, on the consumer thread itself, **handoff**:

```
consumer thread:  cache lookup → breaker → bulkhead → global permit → claim (1 UPDATE) → submit → wait ≤ 500 ms
                                                                                              │
delivery thread (virtual):                       wait for token (≤ 5 s) → HTTP (≤ 10 s) → record result
```

If the attempt is not finished after `handoff-after` (500 ms), the consumer stops waiting and forwards the
record to the next retry topic, with a due time after the attempt must have finished
(token wait + request timeout). The attempt keeps running and records its own result. When the retry copy
arrives it finds `SUCCEEDED` (skipped) or `RETRYING` (tried again), or, if the worker died, an expired lease
(reclaimed).

The consumer thread therefore never blocks for longer than `handoff-after`, and a slow endpoint can do that
at most `bulkhead` times before its records are deferred at no cost.

## Evidence

`NoisyNeighbourIsolationTest` puts endpoint A (10 s latency) and endpoint B (healthy) on the **same
partition** with **one** consumer thread, then sends 200 B and 20 A deliveries over about 4 s. B's p95 from
publish to receipt must be under 1 s. Two runs measured p95 = 618 ms and 670 ms (p50 128–174 ms, max 748–795 ms) on the dev
laptop described in `docs/benchmarks.md`, while A received only 4 requests (its bulkhead in the test is 4).
Without handoff, each A record would hold the partition for 10 s.

## Trade-offs

- **Deferrals use Kafka retry slots.** Circuit-open, bulkhead-full, rate-limited and handed-off records move
  one step along the retry chain without an HTTP attempt. Deferrals are not recorded as attempts in
  `delivery_attempts`, but an endpoint that is throttled or broken for longer than the whole chain
  (about 13 min by default) ends on the DLT, from where it can be bulk-replayed. A dedicated delay topic that
  does not consume attempts is on the roadmap.
- Rate-limited attempts wait on a virtual thread (cheap) holding a bulkhead permit. Sustained traffic above
  the limit therefore turns into deferrals, which is back-pressure rather than unbounded queueing.
- Guards are evicted after 1 h idle or beyond 10,000 endpoints. An evicted breaker forgets its history, which
  is safe. An evicted bulkhead could briefly admit more than 10 concurrent attempts if the endpoint is hit
  during eviction, which is harmless at that scale.
- Metrics are aggregated (`webhook.circuitbreaker.state{state}`), not tagged per endpoint, to bound
  cardinality. Per-endpoint health comes from `GET /v1/deliveries?endpointId=`.
