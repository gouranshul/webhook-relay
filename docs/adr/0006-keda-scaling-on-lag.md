# ADR 0006: Scale the dispatcher on Kafka consumer lag with KEDA (HPA on CPU as fallback)

- Status: accepted
- Date: 2026-10-09

## Context

The dispatcher spends almost all of its time waiting: on receivers (50 ms to 10 s per request) and on Postgres.
With detached attempts on virtual threads (ADR 0004, ADR 0005), a dispatcher can hold hundreds of attempts in
flight at a few percent CPU. CPU is therefore a poor signal. In the calibration run the dispatcher used at most
7 of 12 cores while the backlog grew. And the signal that matters to users, "how far behind are we", is not
visible to a CPU-based HPA at all.

The API is a plain request/response service and scales on CPU or request rate like any other.

## Decision

- The Helm chart ships a KEDA `ScaledObject` for the dispatcher with a `kafka` trigger on consumer group
  `webhook-dispatcher` (the `@KafkaListener` id), topic `webhook.deliveries`, `lagThreshold` 200 per replica.
  Bounds are 1 to 8 replicas, a 15 s polling interval and a 120 s cooldown.
- `allowIdleConsumers: false`. Each pod runs `concurrency` consumers (6 in the chart), and the main topic has
  48 partitions, so 8 pods × 6 consumers = 48 is the useful maximum. More pods would sit idle.
- When KEDA is not installed, `autoscaling.hpa.enabled=true` renders a CPU-based `HorizontalPodAutoscaler`
  instead. The chart refuses to render both, because they would fight over `replicas`.
- Retry topics are not part of the trigger. Their records are *intentionally* delayed, so lag there is mostly
  waiting, not work. Scaling on it would add pods that sit in partition pauses.

## Consequences

- Scale-out reacts to backlog regardless of why it formed (traffic spike, slow receivers, a restart).
- Partition count is the ceiling on useful consumers; raising it later requires repartitioning, which reshuffles
  keys and briefly breaks per-endpoint order. 48 leaves room for growth.
- KEDA needs network access to the brokers and, on secured clusters, a `TriggerAuthentication` (not included:
  the local Kafka has no auth).
- The outbox relay lives in the API, so ingest bursts show up first as `outbox.lag.seconds` and only then as
  Kafka lag. The API's HPA (CPU) and the relay's worker count cover that side.
