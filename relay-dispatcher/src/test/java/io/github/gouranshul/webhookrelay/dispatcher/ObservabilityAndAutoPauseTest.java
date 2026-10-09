package io.github.gouranshul.webhookrelay.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gouranshul.webhookrelay.common.Topics;
import io.github.gouranshul.webhookrelay.common.model.DeliveryMessage;
import io.github.gouranshul.webhookrelay.common.model.DeliveryStatus;
import io.github.gouranshul.webhookrelay.receiver.Behavior;
import io.github.gouranshul.webhookrelay.receiver.HookState;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import tools.jackson.databind.JsonNode;

@TestPropertySource(properties = "relay.auto-pause.consecutive-dlt=3")
class ObservabilityAndAutoPauseTest extends DispatcherIntegrationTest {

    @Autowired
    MeterRegistry registry;

    @Test
    void propagatesTraceContextFromKafkaHeaderToTheWebhookRequest() {
        TestEndpoint endpoint = endpoint(Behavior.healthy());
        DeliveryMessage message = insertDelivery(endpoint, "order.created", "{\"order\":1}");
        String traceId = "4bf92f3577b34da6a3ce929d0e0e4736";

        publish(message, "00-" + traceId + "-00f067aa0ba902b7-01");

        awaitStatus(message.deliveryId(), DeliveryStatus.SUCCEEDED, Duration.ofSeconds(20));
        HookState.Received received = hook(endpoint).recent().getFirst();
        assertThat(received.traceparent()).as("same trace, new span").startsWith("00-" + traceId + "-")
                .doesNotEndWith("00f067aa0ba902b7-01");
    }

    @Test
    void autoPausesEndpointAfterConsecutiveDeadLettersAndEmitsEvent() {
        TestEndpoint endpoint = endpoint(new Behavior(0, 0, 0, 410, null, List.of(410, 410, 410, 410), null));
        try (KafkaConsumer<String, String> consumer = systemEventsConsumer()) {
            List<DeliveryMessage> failing = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                failing.add(enqueue(endpoint));
            }
            await().atMost(Duration.ofSeconds(30))
                    .until(() -> failing.stream().allMatch(m -> deadLettered(m.deliveryId())));
            await().atMost(Duration.ofSeconds(10)).until(() -> "PAUSED".equals(endpointStatus(endpoint.id())));

            JsonNode event = awaitSystemEvent(consumer, endpoint.id());
            assertThat(event.get("type").asString()).isEqualTo("endpoint.paused");
            assertThat(event.get("consecutiveDlt").asInt()).isEqualTo(3);
        }

        // Further deliveries are parked, not attempted.
        DeliveryMessage next = enqueue(endpoint);
        awaitStatus(next.deliveryId(), DeliveryStatus.PARKED, Duration.ofSeconds(20));
        assertThat(hook(endpoint).stats().requests()).isEqualTo(3);
        assertThat(registry.get("webhook.endpoint.auto_paused").counter().count()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void successResetsTheConsecutiveDeadLetterCount() {
        TestEndpoint endpoint = endpoint(scripted(410, 410, 200, 410, 410));
        for (int i = 0; i < 5; i++) {
            DeliveryMessage m = enqueue(endpoint);
            await().atMost(Duration.ofSeconds(20)).until(() -> status(m.deliveryId()).isTerminal()
                    && (status(m.deliveryId()) == DeliveryStatus.SUCCEEDED || deadLettered(m.deliveryId())));
        }
        assertThat(endpointStatus(endpoint.id())).isEqualTo("ACTIVE");
        assertThat(jdbc.sql("SELECT consecutive_dlt FROM endpoints WHERE id = ?").param(endpoint.id())
                .query(Integer.class).single()).isEqualTo(2);
    }

    @Test
    void exposesDeliveryAndKafkaConsumerMetrics() {
        TestEndpoint endpoint = endpoint(Behavior.healthy());
        DeliveryMessage message = enqueue(endpoint);
        awaitStatus(message.deliveryId(), DeliveryStatus.SUCCEEDED, Duration.ofSeconds(20));

        assertThat(registry.find("webhook.delivery.duration").tag("outcome", "success").timer()).isNotNull();
        assertThat(registry.find("webhook.delivery.attempts").tag("attempt", "1").counters()).isNotEmpty();
        assertThat(registry.find("webhook.circuitbreaker.state").gauges()).hasSize(3);
        // Kafka client metrics bound by Spring Kafka's Micrometer listener; lag is what KEDA scales on.
        await().atMost(Duration.ofSeconds(30))
                .until(() -> !registry.find("kafka.consumer.fetch.manager.records.lag.max").gauges().isEmpty());
    }

    private String endpointStatus(UUID id) {
        return jdbc.sql("SELECT status FROM endpoints WHERE id = ?").param(id).query(String.class).single();
    }

    private JsonNode awaitSystemEvent(KafkaConsumer<String, String> consumer, UUID endpointId) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                if (endpointId.toString().equals(record.key())) {
                    return mapper.readTree(record.value());
                }
            }
        }
        throw new AssertionError("no endpoint.paused event for " + endpointId);
    }

    private static KafkaConsumer<String, String> systemEventsConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "system-events-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of(Topics.SYSTEM_EVENTS));
        return consumer;
    }
}
