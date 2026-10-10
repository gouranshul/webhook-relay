# Benchmarks

Every number on this page comes from a run whose raw output is committed under [`load/results/`](../load/results/).
Nothing here is extrapolated. Where a run was invalid, it is kept and labelled as such.

## Machine

All components ran on **one laptop**, competing for the same CPU and memory: Kafka, Postgres, Redis, relay-api,
relay-dispatcher, the mock receiver, Prometheus, Grafana, Jaeger and the k6 load generator itself.

| | |
|---|---|
| CPU | Intel Core i5-1235U (2 performance + 8 efficiency cores, 12 threads), laptop part, 15 W base power |
| RAM | 16 GB |
| OS | Windows 11 Pro 10.0.26200 |
| Docker | Docker Desktop 29.8.2, WSL2 VM with 12 vCPUs and 7.6 GiB of memory |
| JVM | Temurin 25.0.4.1 (`eclipse-temurin:25.0.4.1_1-jre-alpine`), default GC (G1), `-XX:MaxRAMPercentage=75`, no container memory limit |
| Images | `apache/kafka:4.3.1` (single KRaft node), `postgres:18.6-alpine`, `redis:8.10.2-alpine`, `grafana/k6:2.3.0` |

**What this means:** these are single-node, single-instance numbers on a thermally limited laptop with a
replication factor of 1. They show relative behaviour (virtual vs platform threads, fast vs slow receivers,
where the bottleneck is). They are not a claim about production capacity, which would come from more cores,
more dispatcher pods (KEDA) and a real Kafka cluster.

## Method

`load/run-benchmark.sh <name>` (see the script for all knobs):

1. Recreates `relay-api`, `relay-dispatcher` and `mock-receiver` with the run's settings, truncates every relay
   table and flushes Redis, so runs are independent.
2. k6 (`load/ingest.js`) registers 12 endpoints (4 per event type: `order.created`, `user.signed_up`,
   `invoice.paid`), so **each event fans out to 4 deliveries**. Every hook knows its secret and verifies signatures.
3. Ramp to the target event rate over 30 s, hold for 3 min (`ramping-arrival-rate`, unique `Idempotency-Key`
   per request). Thresholds: < 1 % failed requests, publish p95 < 250 ms and p99 < 500 ms, < 1 % dropped iterations.
4. Wait until **every** delivery is terminal, then read the outcome from Postgres: deliveries by status,
   attempts, deliveries completed per second during the hold, and **accept → delivered latency** (`completed_at -
   created_at` of the delivery row). That latency includes the outbox poll, Kafka, the dispatcher and the
   receiver's own 50–200 ms.
5. Dispatcher heap, threads, in-flight attempts and CPU come from Prometheus; container memory from `docker stats`.

Receiver: 10 % random 503s, uniform 50–200 ms latency (unless stated). Retry max delay is capped at 2 min for
benchmark runs (`RELAY_RETRY_MAX_DELAY=2m`) so runs drain in minutes. Tracing sampling is 1 %.

Reproduce:

```bash
cp .env.example .env   # fill in placeholders
docker compose up -d --build
RATE=60 load/run-benchmark.sh vt-60
VIRTUAL_THREADS_ENABLED=false RELAY_DISPATCHER_EXECUTOR=platform RELAY_DISPATCHER_PLATFORM_POOL_SIZE=200 RATE=100 load/run-benchmark.sh pt-100-r2
RECEIVER_MIN_LATENCY_MS=1000 RECEIVER_MAX_LATENCY_MS=2000 RATE=50 load/run-benchmark.sh vt-slow-50
```

## Validity: what invalidated earlier runs

Three things went wrong before these numbers, and each is kept as evidence:

1. **A real bug.** The first full batch revealed that retry records were routed to non-existent partitions
   (48 main vs 12 retry partitions). Every delivery still terminated (via the lease sweeper), but retries took
   hours. Fixed and regression-tested; all runs below are on the fixed code
   ([`retry-partition-bug.md`](../load/results/retry-partition-bug.md)).
2. **Host standby.** Windows Modern Standby froze the Docker VM mid-run, which looked like stuck deliveries.
   Every published run's time window (in its report) was checked against the Kernel-Power standby events: **no
   standby occurred inside any published window.**
3. **Battery.** Three runs before 12:18 UTC were on battery and throttled (for example `vt-100-battery`: 116
   deliveries/s and failed thresholds, against 412/s for the same scenario on AC). They are kept as
   `*-battery.md` and excluded. **All published runs ran on AC power** (power-source change event at 12:18:41 UTC,
   none after).

## Results

All runs: 12 endpoints (fan-out 4), 30 s ramp + 3 min hold, 2026-10-10, AC power, no standby. Latency is
accept → delivered (see Method). "First attempt" covers deliveries that succeeded on try 1; "all" includes
retries (10 % of attempts fail, and a retry waits ≥ 1 s by design).

### Fast receivers (50–200 ms, 10 % 503s)

| Run | Threads | Events accepted /s | Deliveries /s (hold) | First attempt p50 / p95 / p99 | All successes p50 / p95 / p99 | Ingest p95 / p99 | k6 thresholds |
|---|---|---|---|---|---|---|---|
| [`vt-60`](../load/results/vt-60.md) | virtual | 56.1 | **239.4** | **205 / 407 / 604 ms** | 214 ms / 1.8 s / 5.4 s | 75 / 147 ms | all pass |
| [`vt-100-r2`](../load/results/vt-100-r2.md) | virtual | 93.3 | 412.1 | 547 ms / 37.0 s / 47.1 s | 1.4 / 37.4 / 47.5 s | 234 / 465 ms | all pass |
| [`pt-100-r2`](../load/results/pt-100-r2.md) | platform, pool 200 | 91.1 | 411.3 | 18.5 / 67.7 / 84.6 s | 18.8 / 68.0 / 85.2 s | 1,769 / 4,099 ms | latency + dropped iterations fail |
| [`vt-200-overload`](../load/results/vt-200-overload.md) | virtual | 180.3 | 416.7 | 59.2 / 175.0 / 208.4 s | 59.5 / 175.2 / 209.2 s | 2,206 / 3,467 ms | latency + dropped iterations fail |

| Run | Final state of every delivery | Receiver duplicates | Dispatcher heap peak | Live platform threads (peak) | Attempts in flight (peak) | Dispatcher container memory (peak) |
|---|---|---|---|---|---|---|
| `vt-60` | 47,155 SUCCEEDED, 1 FAILED (6 × 503, retries exhausted) | 0 | 159 MB | 129 | 67 | 434 MiB |
| `vt-100-r2` | 78,468 SUCCEEDED | 0 | 287 MB | 137 | 195 | 642 MiB |
| `pt-100-r2` | 77,236 SUCCEEDED | 0 | 261 MB | 431 | 600 | 705 MiB |
| `vt-200-overload` | 152,599 SUCCEEDED, 1 FAILED (retries exhausted) | 0 | 1,445 MB | 352 | 390 | 596 MiB |

### Slow receivers (1–2 s, 10 % 503s): virtual vs platform threads

| Run | Threads | Deliveries /s (hold) | First attempt p50 / p95 / p99 | Final state | Heap peak | Container memory peak |
|---|---|---|---|---|---|---|
| [`vt-slow-50`](../load/results/vt-slow-50.md) | virtual | **200.2** (= offered) | **1.56 / 2.00 / 2.07 s** | 39,296 SUCCEEDED | **259 MB** | 565 MiB |
| [`pt-slow-50`](../load/results/pt-slow-50.md) | platform, pool 200 | 116.9 | 27.3 / 214.7 / 325.5 s | 39,112 SUCCEEDED, **184 FAILED** | 3,188 MB | 4.4 GiB |

Across all six published runs: **0 lost deliveries** (every delivery ended SUCCEEDED or FAILED/DLT and the
receiver's own counters match), **0 duplicate deliveries** observed by the receiver, **0 signature failures**
(none of the 481,965 HTTP attempts was answered 401: every attempt was either a success or a retryable 503).

## Interpretation

- **Sustainable load on this laptop is about 240 deliveries/s** (60 events/s × 4) with p99 accept → delivered of
  604 ms for first attempts, and every k6 threshold met.
- **The ceiling is about 410–417 deliveries/s, and it is not set by the thread model.** Virtual threads at 100
  events/s, platform threads at 100 events/s, and virtual threads at 200 events/s all top out at 411–417/s. At
  that point all ten containers share one 15 W laptop CPU (the dispatcher used 4–9 of 12 cores; the rest went to
  Postgres, Kafka, the API and k6 itself). Beyond the ceiling the backlog queues in Kafka and latency grows into
  minutes, but nothing is lost: the overload run drained 152,600 deliveries completely within 308 s of the load
  stopping. Production scales this out with dispatcher pods (KEDA, ADR 0006).
- **With fast receivers, virtual and platform threads deliver the same throughput.** A 200-thread pool covers
  the ~60–200 concurrent attempts this needs. The platform run needed 3× the platform threads (431 vs 137) and,
  in this single pair, showed worse delivery latency and degraded the co-located API (dropped iterations). The
  laptop's run-to-run variance is large (see the battery runs), so treat that latency gap as indicative, not
  proven.
- **With slow receivers the difference is decisive.** At 1–2 s per request, 200 deliveries/s needs ~300 attempts
  in flight. Virtual threads ran 374 at once on 259 MB of heap and kept p99 at 2.07 s, which is the receiver's own
  latency. The 200-thread pool capped throughput at 117/s (theory: 200 / 1.5 s ≈ 133/s). Attempts queued for
  minutes, and the per-endpoint bulkheads filled with queued attempts, so new records were deferred until they
  exhausted their retry budget: 184 deliveries ended on the DLT without the receiver ever having failed them.
  Heap in use peaked at 3.2 GB (G1 grows the heap under this churn, so this is not the live set).
- **Limits of these numbers:** a single laptop, a single Kafka broker with replication factor 1, one dispatcher
  and one API instance, all sharing a CPU with the load generator, and one run per configuration. They support
  relative conclusions (what scales, where the bottleneck is) rather than absolute capacity claims.

## Isolation and chaos (from the test suite)

- **Noisy neighbour** (`NoisyNeighbourIsolationTest`): a 10 s endpoint and a healthy endpoint on the same partition
  with one consumer; healthy endpoint p95 **40 ms** (ADR 0004).
- **Chaos** (`ChaosTest`, [`chaos-final.txt`](../load/results/chaos-final.txt)): Kafka restart, 5 s Postgres
  latency, 60 s receiver outage, Kafka killed for 45 s: 4 × 1,800 deliveries, **0 lost, 0 duplicates**. In the
  Kafka-kill scenario 150 forwards failed and the lease sweeper re-enqueued all 150.
