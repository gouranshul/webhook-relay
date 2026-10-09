# CLAUDE.md

Guidance for AI coding agents (and humans) working in this repository.

## Purpose

`webhook-relay` is a reliable webhook delivery service. It never loses events (transactional
outbox → Kafka), signs every payload (HMAC-SHA256), retries with exponential backoff and jitter
(non-blocking Kafka retry topics + DLT), isolates slow or failing receivers (per-endpoint circuit
breaker, bulkhead and Redis token-bucket rate limit), and backs its throughput claims with k6 load
tests and chaos tests.

## Modules

| Module | What it owns |
|---|---|
| `relay-common` | Event/delivery model, `WebhookSigner` / `WebhookVerifier`, AES-GCM `SecretCipher`, topic names, Flyway migrations (shared schema) |
| `relay-api` | REST API (`/v1/endpoints`, `/v1/events`, `/v1/deliveries`, replay), idempotency keys, the transactional outbox and `OutboxRelay` |
| `relay-dispatcher` | Kafka consumers on virtual threads, `@RetryableTopic` retries + DLT, signed HTTP delivery (JDK `HttpClient`), per-endpoint resilience |
| `mock-receiver` | Flaky receiver for tests and load: configurable failure rate, latency, status codes; verifies signatures; counts duplicates |
| `relay-e2e` | End-to-end chaos tests (Testcontainers + Toxiproxy): all three apps in one JVM, asserting zero lost events |
| `load/` | k6 scripts |
| `deploy/` | Helm chart (KEDA or HPA), Grafana dashboard, Prometheus config |
| `docs/` | ADRs and measured benchmarks |

## Commands

```bash
./mvnw verify                                  # build + all tests (needs Docker for Testcontainers)
./mvnw -pl relay-dispatcher -am verify         # one module and what it depends on
./mvnw -pl relay-common test -Dtest=WebhookSignatureTest
cp .env.example .env                           # then replace every placeholder
docker compose up -d --build                   # full local stack
docker run --rm -v "$PWD:/repo" zricethezav/gitleaks:v8.30.1 git /repo   # before every push
RATE=100 load/run-benchmark.sh my-run         # k6 benchmark against the compose stack (see docs/benchmarks.md)
```

Ports: api 8080, dispatcher 8081, mock-receiver 8090, Prometheus 9090, Grafana 3000, Jaeger 16686, Kafka 29092 (host).

## Hard rules

1. Generic, original work only. No company names, internal domains or proprietary code. Nothing finance,
   valuation or fund related. Demo events are generic SaaS examples (`order.created`, `user.signed_up`, `invoice.paid`).
2. Commit identity must be a personal/noreply address, never a work domain.
3. Never commit secrets. Configuration comes from environment variables; `.env.example` holds placeholders only.
   Run gitleaks before every push.
4. Verify every dependency and image version against Maven Central, Docker Hub or official docs before pinning it.
   If an API changed in the current release, follow the current docs and record the difference in `docs/adr`.
5. `./mvnw verify` must pass before every commit that changes code.
6. Never invent benchmark numbers. Every number in README/docs comes from a run that was actually executed,
   with machine specs recorded next to it.
7. Conventional Commits, small and meaningful.

## Conventions

- Java 25, Spring Boot 4.1, `JdbcClient` (no JPA). Records for DTOs and messages.
- Each app reads its own config file (`relay-api.yml`, `relay-dispatcher.yml`, `mock-receiver.yml`) via
  `spring.config.name`, so apps can share a classpath in tests. Tests must pass `XxxApplication.CONFIG_NAME`.
- The dispatcher never waits for HTTP on the consumer thread: attempts run on virtual threads, forward their
  own failures via `RetryForwarder`, and the delivery lease + `LeaseSweeper` cover crashes (ADR 0004).
- Package root: `io.github.gouranshul.webhookrelay.<module>`.
- Delivery semantics are at-least-once. Receivers must dedupe on `Webhook-Id`. Never claim exactly-once.
- Anything per-endpoint that lives in memory (circuit breakers, bulkheads, caches) must be bounded.
- Every outbound call has a connect and a request timeout.
