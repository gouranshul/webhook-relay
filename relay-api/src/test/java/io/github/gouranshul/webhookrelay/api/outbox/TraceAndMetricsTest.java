package io.github.gouranshul.webhookrelay.api.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gouranshul.webhookrelay.api.ApiIntegrationTest;
import io.github.gouranshul.webhookrelay.api.config.RelayApiProperties;
import io.github.gouranshul.webhookrelay.common.Topics;
import io.github.gouranshul.webhookrelay.common.model.OutboxShards;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

class TraceAndMetricsTest extends ApiIntegrationTest {

    @Autowired
    OutboxRepository repository;
    @Autowired
    KafkaTemplate<String, String> kafka;
    @Autowired
    PlatformTransactionManager txManager;
    @Autowired
    RelayApiProperties properties;
    @Autowired
    OutboxMetrics outboxMetrics;
    @Autowired
    MeterRegistry registry;

    @Test
    void traceparentTravelsFromHttpRequestThroughTheOutboxToTheKafkaHeader() {
        String type = uniqueType("order");
        UUID endpoint = createEndpoint(List.of(type));
        String traceId = "0af7651916cd43dd8448eb211c80319c";

        var accepted = post("/v1/events", "{\"type\":\"%s\",\"data\":{}}".formatted(type),
                "traceparent", "00-" + traceId + "-b7ad6b7169203331-01");
        UUID eventId = UUID.fromString(json(accepted).get("id").asString());

        String stored = jdbc.sql("SELECT traceparent FROM events WHERE id = ?").param(eventId)
                .query(String.class).single();
        assertThat(stored).startsWith("00-" + traceId + "-");

        OutboxRelay relay = new OutboxRelay("trace-test", repository, kafka, mapper, new TransactionTemplate(txManager),
                properties.outbox(), new SimpleMeterRegistry());
        int shard = jdbc.sql("SELECT shard FROM outbox WHERE endpoint_id = ?").param(endpoint)
                .query(Integer.class).single();
        assertThat(shard).isBetween(0, OutboxShards.COUNT - 1);
        assertThat(relay.publishShard(shard)).isPositive();

        try (KafkaConsumer<String, String> consumer = consumer()) {
            consumer.subscribe(List.of(Topics.DELIVERIES));
            Header header = null;
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (header == null && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    if (endpoint.toString().equals(record.key())) {
                        header = record.headers().lastHeader("traceparent");
                    }
                }
            }
            assertThat(header).isNotNull();
            assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo(stored);
        }
    }

    @Test
    void reportsOutboxBacklogGauges() {
        String type = uniqueType("user");
        createEndpoint(List.of(type));
        post("/v1/events", "{\"type\":\"%s\",\"data\":{}}".formatted(type));
        jdbc.sql("UPDATE outbox SET created_at = now() - interval '42 seconds' WHERE published_at IS NULL").update();

        outboxMetrics.sample();

        assertThat(registry.get("outbox.pending").gauge().value()).isGreaterThanOrEqualTo(1);
        assertThat(registry.get("outbox.lag.seconds").gauge().value()).isGreaterThanOrEqualTo(42);
    }

    private static KafkaConsumer<String, String> consumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "trace-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }
}
