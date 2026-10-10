# ADR 0005: Virtual threads for consumers and webhook attempts

- Status: accepted
- Date: 2026-10-09

## Context

A webhook dispatcher is a waiting machine. Each attempt spends roughly 99 % of its life blocked: on the
receiver (50 ms to 10 s), on Postgres, on Redis. With one platform thread per in-flight attempt, concurrency is
capped by thread count, and each platform thread reserves about 1 MB of stack. With an async/reactive stack the
cap goes away, but every line of code (JDBC, error handling, tracing context) has to be non-blocking.

Java 25 virtual threads remove the trade-off: blocking code (JDK `HttpClient.send`, JDBC, Lettuce sync calls)
parks the virtual thread and frees the carrier.

## Decision

- `spring.threads.virtual.enabled=true` (env `VIRTUAL_THREADS_ENABLED`) for both apps. Kafka listener containers,
  Tomcat and `@Scheduled` tasks run on virtual threads.
- Each webhook attempt runs on its own virtual thread (`relay.dispatcher.executor=virtual`). Concurrency is
  bounded by policy (per-endpoint bulkhead, global in-flight cap of 2,000), not by a thread pool.
- A fixed platform pool (`relay.dispatcher.executor=platform`, `platform-pool-size`) remains available for the
  benchmark comparison and as an escape hatch.

## A bug that virtual threads hide, and that a platform pool exposed

The first platform-thread benchmark run deadlocked. The fixed 200-thread attempt pool was also given to the JDK
`HttpClient` as its executor. With 600 attempts queued and all 200 threads blocked in `send()`, no thread was
left to process the responses those threads were waiting for. Every call timed out, deliveries were
dead-lettered, and endpoint auto-pause parked about 77,000 deliveries
([evidence](../../load/results/pt-100-deadlock.md)). With virtual threads the executor is unbounded, so the
same wiring never starved. That is exactly why it went unnoticed.

Fix: the `HttpClient` gets its own executor (a separate virtual-thread executor, or the JDK default cached pool
in platform mode). `PlatformExecutorTest` reproduces the scenario with a 2-thread pool: without the fix the test
JVM hung and had to be killed; with it, all 10 deliveries succeed.

## Measured comparison

Same scenario, same laptop, one run per configuration; `VIRTUAL_THREADS_ENABLED=true/false` (Tomcat, Kafka
listeners, schedulers) and the attempt executor (virtual threads vs a fixed pool of 200 platform threads).
Details and caveats: [docs/benchmarks.md](../benchmarks.md).

| Receivers | Threads | Deliveries/s | First-attempt latency p50 / p99 | Heap peak | Live platform threads | Outcome |
|---|---|---|---|---|---|---|
| fast (50–200 ms), 100 events/s | virtual | 412 | 0.55 s / 47 s | 287 MB | 137 | all delivered |
| fast (50–200 ms), 100 events/s | platform (200) | 411 | 18.5 s / 85 s | 261 MB | 431 | all delivered; API ingest degraded |
| slow (1–2 s), 50 events/s | virtual | **200** | **1.56 s / 2.07 s** | **259 MB** | 364 | all delivered |
| slow (1–2 s), 50 events/s | platform (200) | 117 | 27 s / 326 s | 3,188 MB | 395 | 184 dead-lettered after their retry budget ran out |

At 100 events/s both are at the laptop's ceiling (~410 deliveries/s, shared CPU), so throughput is equal. The
point of virtual threads shows with slow receivers: concurrency follows demand (374 attempts in flight) instead of
a pool size, at a fraction of the memory.

## Consequences

- No reactive types anywhere; stack traces and `ThreadLocal`-based tracing context behave normally.
- Pinning: JDK 24+ no longer pins on `synchronized` (JEP 491). Blocking native calls (Kafka's NIO selector) still
  occupy a carrier while they block. The Kafka consumers are long-lived and few (main topic plus retry topics),
  so this costs a handful of carriers.
- Thread dumps need `jcmd <pid> Thread.dump_to_file` because `jstack` does not show virtual threads. We
  learned this while debugging the async-listener experiment (ADR 0004).
