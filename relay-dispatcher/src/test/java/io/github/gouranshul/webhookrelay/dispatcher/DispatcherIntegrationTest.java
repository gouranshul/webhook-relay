package io.github.gouranshul.webhookrelay.dispatcher;

import static org.awaitility.Awaitility.await;

import io.github.gouranshul.webhookrelay.common.Topics;
import io.github.gouranshul.webhookrelay.common.crypto.SecretCipher;
import io.github.gouranshul.webhookrelay.common.crypto.SigningSecrets;
import io.github.gouranshul.webhookrelay.common.model.DeliveryMessage;
import io.github.gouranshul.webhookrelay.common.model.DeliveryStatus;
import io.github.gouranshul.webhookrelay.common.testing.Containers;
import io.github.gouranshul.webhookrelay.receiver.Behavior;
import io.github.gouranshul.webhookrelay.receiver.EmbeddedReceiver;
import io.github.gouranshul.webhookrelay.receiver.HookState;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

/**
 * Real Postgres and Kafka, and the mock receiver running in-process. Retry delays and timeouts are shortened
 * so the full retry chain runs in seconds. Subclasses may override properties with {@code @TestPropertySource}.
 * Each class gets a fresh context, closed afterwards, so only one dispatcher consumes at a time.
 */
@SpringBootTest(classes = DispatcherApplication.class, properties = {
    "spring.flyway.enabled=true",
    "relay.endpoints.allow-private-networks=true",
    "relay.endpoints.cache-ttl=300ms",
    "relay.dispatcher.concurrency=2",
    "relay.dispatcher.connect-timeout=1s",
    "relay.dispatcher.request-timeout=1s",
    "relay.dispatcher.lease-margin=1s",
    "relay.retry.attempts=4",
    "relay.retry.initial-delay=300ms",
    "relay.retry.multiplier=2",
    "relay.retry.max-delay=2s",
    "relay.retry.jitter=0.2",
    "relay.resilience.handoff-after=2s",
    "relay.resilience.rate-limit-max-wait=2s",
    "relay.kafka.partitions=3"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class DispatcherIntegrationTest {

    @ServiceConnection
    protected static final PostgreSQLContainer POSTGRES = Containers.postgres();

    @ServiceConnection
    protected static final KafkaContainer KAFKA = Containers.kafka();

    protected static final org.testcontainers.containers.GenericContainer<?> REDIS = Containers.redis();

    protected static final EmbeddedReceiver RECEIVER = EmbeddedReceiver.start();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("relay.crypto.key", () -> Containers.SECRET_KEY);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired
    protected JdbcClient jdbc;
    @Autowired
    protected KafkaTemplate<String, String> kafka;
    @Autowired
    protected ObjectMapper mapper;
    @Autowired
    protected SecretCipher cipher;

    /** An endpoint row plus a receiver hook that knows its secret and verifies signatures. */
    public record TestEndpoint(UUID id, String hook, String secret) {
    }

    protected TestEndpoint endpoint(Behavior behavior) {
        return endpoint(behavior, 1000);
    }

    protected TestEndpoint endpoint(Behavior behavior, int rateLimitPerSec) {
        return endpoint(UUID.randomUUID(), behavior, rateLimitPerSec);
    }

    protected TestEndpoint endpoint(UUID id, Behavior behavior, int rateLimitPerSec) {
        String hook = "hook-" + UUID.randomUUID();
        String secret = SigningSecrets.generate();
        UUID secretId = UUID.randomUUID();
        jdbc.sql("INSERT INTO endpoint_secrets (id, key_id, ciphertext) VALUES (?, ?, ?)")
                .params(secretId, cipher.keyId(), cipher.encrypt(secretId, secret)).update();
        jdbc.sql("""
                        INSERT INTO endpoints (id, url, secret_ref, event_types, rate_limit_per_sec)
                        VALUES (?, ?, ?, ARRAY['*'], ?)""")
                .params(id, RECEIVER.url(hook), secretId, rateLimitPerSec).update();
        RECEIVER.state().configure(hook, behavior.withSecret(secret));
        return new TestEndpoint(id, hook, secret);
    }

    /** Writes the event and a PENDING delivery (as the API would), then publishes the record (as the relay would). */
    protected DeliveryMessage enqueue(TestEndpoint endpoint) {
        DeliveryMessage message = insertDelivery(endpoint, "order.created", "{\"order_id\":\"" + UUID.randomUUID() + "\"}");
        publish(message, null);
        return message;
    }

    protected DeliveryMessage insertDelivery(TestEndpoint endpoint, String type, String payload) {
        UUID eventId = UUID.randomUUID();
        UUID deliveryId = UUID.randomUUID();
        Instant created = jdbc.sql("INSERT INTO events (id, event_type, payload) VALUES (?, ?, CAST(? AS jsonb)) RETURNING created_at")
                .params(eventId, type, payload).query(Timestamp.class).single().toInstant();
        jdbc.sql("INSERT INTO deliveries (id, webhook_id, event_id, endpoint_id, status) VALUES (?, ?, ?, ?, 'PENDING')")
                .params(deliveryId, deliveryId, eventId, endpoint.id()).update();
        return new DeliveryMessage(deliveryId, deliveryId, eventId, endpoint.id(), type, payload, created);
    }

    protected void publish(DeliveryMessage message, String traceparent) {
        var record = new ProducerRecord<>(Topics.DELIVERIES, message.endpointId().toString(),
                mapper.writeValueAsString(message));
        if (traceparent != null) {
            record.headers().add("traceparent", traceparent.getBytes(StandardCharsets.UTF_8));
        }
        try {
            kafka.send(record).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    protected DeliveryStatus status(UUID deliveryId) {
        return DeliveryStatus.valueOf(jdbc.sql("SELECT status FROM deliveries WHERE id = ?")
                .param(deliveryId).query(String.class).single());
    }

    protected void awaitStatus(UUID deliveryId, DeliveryStatus expected, Duration timeout) {
        await().atMost(timeout).pollInterval(Duration.ofMillis(100))
                .until(() -> status(deliveryId) == expected);
    }

    protected boolean deadLettered(UUID deliveryId) {
        return jdbc.sql("SELECT dlt_at IS NOT NULL FROM deliveries WHERE id = ?")
                .param(deliveryId).query(Boolean.class).single();
    }

    public record Attempt(int number, String outcome, Integer statusCode, int latencyMs, String error, Instant at) {
    }

    protected List<Attempt> attempts(UUID deliveryId) {
        return jdbc.sql("""
                        SELECT attempt_number, outcome, status_code, latency_ms, error, attempted_at
                          FROM delivery_attempts WHERE delivery_id = ? ORDER BY attempt_number""")
                .param(deliveryId)
                .query((rs, n) -> new Attempt(rs.getInt(1), rs.getString(2), (Integer) rs.getObject(3), rs.getInt(4),
                        rs.getString(5), rs.getTimestamp(6).toInstant()))
                .list();
    }

    protected HookState hook(TestEndpoint endpoint) {
        return RECEIVER.state().hook(endpoint.hook());
    }

    protected static Behavior scripted(Integer... statuses) {
        return new Behavior(0, 0, 0, 503, null, List.of(statuses), null);
    }
}
