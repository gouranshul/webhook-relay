# ADR 0007: Notes on current platform versions (API differences from older docs)

- Status: accepted
- Date: 2026-10-09

Every version was checked against Maven Central, Docker Hub or GHCR before pinning. These are the places
where the current release differs from what older tutorials, docs or memory would suggest.

| Area | Older API / assumption | What the pinned version actually does | Where it matters |
|---|---|---|---|
| Spring Boot 4.1.1 | `spring-boot-starter-web` | Modular starters: `spring-boot-starter-webmvc`, `-kafka`, `-flyway`, `-opentelemetry`, plus `*-test` starters such as `spring-boot-starter-webmvc-test` | all POMs |
| Spring Boot 4.1.1 | `org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc` | `org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc` | API tests |
| Spring Boot 4.1.1 | `management.otlp.tracing.endpoint` | `management.opentelemetry.tracing.export.otlp.endpoint` (old key still listed, deprecated) | `application.yml` |
| Spring Boot 4.1.1 + OTel starter | Metrics only go where you point them | `micrometer-registry-otlp` comes with the starter and **pushes to localhost:4318 by default**; disabled with `management.otlp.metrics.export.enabled=false` because Prometheus scrapes | `application.yml` |
| Auto-config class names | `org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration` | `org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration` (likewise `kafka.autoconfigure`, `flyway.autoconfigure`, `data.redis.autoconfigure.DataRedisAutoConfiguration`) | `EmbeddedReceiver` |
| Jackson 3 (Boot 4 default) | `com.fasterxml.jackson.databind.ObjectMapper`, checked exceptions | `tools.jackson.databind.ObjectMapper`, unchecked `JacksonException`; annotations stay in `com.fasterxml.jackson.annotation`; `JsonNode#asText()` → `asString()` | API, dispatcher |
| Spring for Apache Kafka 4.1.1 | `@RetryableTopic(backoff = @Backoff(delay, multiplier, random = true))` from Spring Retry | Spring Retry is gone: `backOff = @BackOff(delayString, multiplierString, maxDelayString, jitter)` from `org.springframework.kafka.annotation`; strings accept durations like `1s` | `DeliveryListener` |
| Spring for Apache Kafka 4.1.1 | `random = true` gives each record a random delay | Jitter is applied **once per retry topic at startup**. Per-record jitter needs a custom backoff header ([ADR 0003](0003-non-blocking-retry-topics.md)) | `RetryScheduling` |
| Spring for Apache Kafka 4.1.1 | — | `DeadLetterPublishingRecoverer.addHeadersFunction` composes by calling `setHeadersFunction`, which logs a misleading "Replacing custom headers function" warning; both functions remain active. Logger set to `error`. | dispatcher `application.yml` |
| Testcontainers 2.0.5 | `org.testcontainers:postgresql`, `org.testcontainers.containers.PostgreSQLContainer<?>` | Artifacts are `testcontainers-postgresql`, `testcontainers-kafka`, `testcontainers-toxiproxy`; classes are `org.testcontainers.postgresql.PostgreSQLContainer` (not generic) and `org.testcontainers.kafka.KafkaContainer` (works with `apache/kafka`) | all tests |
| PostgreSQL 18 Docker image | Volume at `/var/lib/postgresql/data` | `PGDATA` is `/var/lib/postgresql/18/docker`; mount the volume at `/var/lib/postgresql` | `docker-compose.yml`, Helm |
| Toxiproxy | `shopify/toxiproxy` on Docker Hub | Docker Hub stops at 2.1.4; current releases are on `ghcr.io/shopify/toxiproxy` (2.12.0) | chaos tests |
| JUnit | JUnit 5 | Boot 4.1 manages JUnit Jupiter **6.0.3**. The Jupiter API (`@Test`, `@ParameterizedTest`) is unchanged for this code base | all tests |
| Maven | 3.9.x | Maven 3.10.0 and 4.0 RCs exist; the wrapper pins 3.9.16, the newest 3.9 GA, for plugin compatibility | `.mvn/wrapper` |
