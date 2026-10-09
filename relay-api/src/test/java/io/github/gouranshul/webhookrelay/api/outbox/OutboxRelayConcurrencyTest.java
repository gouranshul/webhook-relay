package io.github.gouranshul.webhookrelay.api.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gouranshul.webhookrelay.api.ApiIntegrationTest;
import io.github.gouranshul.webhookrelay.api.config.RelayApiProperties;
import io.github.gouranshul.webhookrelay.api.event.EventDtos.PublishEventRequest;
import io.github.gouranshul.webhookrelay.api.event.EventService;
import io.github.gouranshul.webhookrelay.common.Topics;
import io.github.gouranshul.webhookrelay.common.model.DeliveryMessage;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Three relays compete for the same outbox while 10,000 events are ingested. Every delivery must be published
 * exactly once (no failures are injected, so no at-least-once duplicates are expected either), and each
 * endpoint's records must arrive in ingest order.
 */
class OutboxRelayConcurrencyTest extends ApiIntegrationTest {

    private static final int ENDPOINTS = 4;
    private static final int EVENTS_PER_ENDPOINT = 2_500;
    private static final int TOTAL = ENDPOINTS * EVENTS_PER_ENDPOINT;

    @Autowired
    EventService events;
    @Autowired
    OutboxRepository repository;
    @Autowired
    KafkaTemplate<String, String> kafka;
    @Autowired
    PlatformTransactionManager txManager;
    @Autowired
    RelayApiProperties properties;

    @Test
    void threeRelaysPublishTenThousandEventsWithoutLossDuplicatesOrReordering() throws Exception {
        List<String> types = new ArrayList<>();
        Map<UUID, String> endpointTypes = new HashMap<>();
        for (int i = 0; i < ENDPOINTS; i++) {
            String type = uniqueType("order");
            types.add(type);
            endpointTypes.put(createEndpoint(List.of(type)), type);
        }

        List<OutboxRelay> relays = new ArrayList<>();
        List<SimpleMeterRegistry> registries = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            registries.add(new SimpleMeterRegistry());
            relays.add(new OutboxRelay("test-relay-" + i, repository, kafka, mapper, new TransactionTemplate(txManager),
                    properties.outbox(), registries.get(i)));
        }
        relays.forEach(OutboxRelay::start);

        try (KafkaConsumer<String, String> consumer = consumer()) {
            consumer.subscribe(List.of(Topics.DELIVERIES));
            // One sequential producer per endpoint, all running concurrently with the relays.
            try (ExecutorService producers = Executors.newFixedThreadPool(ENDPOINTS)) {
                List<Future<?>> futures = new ArrayList<>();
                for (String type : types) {
                    futures.add(producers.submit(() -> {
                        for (int seq = 0; seq < EVENTS_PER_ENDPOINT; seq++) {
                            events.publish(null, new PublishEventRequest(type,
                                    mapper.readTree("{\"seq\":" + seq + "}")), null);
                        }
                    }));
                }
                for (Future<?> future : futures) {
                    future.get();
                }
            }

            List<DeliveryMessage> received = new ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
            while (received.size() < TOTAL && System.nanoTime() < deadline) {
                collect(consumer, endpointTypes, received);
            }
            // Keep polling briefly so a late duplicate would be caught.
            long grace = System.nanoTime() + Duration.ofSeconds(3).toNanos();
            while (System.nanoTime() < grace) {
                collect(consumer, endpointTypes, received);
            }

            assertThat(received).hasSize(TOTAL);
            Set<UUID> deliveryIds = new HashSet<>();
            received.forEach(m -> deliveryIds.add(m.deliveryId()));
            assertThat(deliveryIds).as("distinct delivery ids").hasSize(TOTAL);

            Map<UUID, Integer> lastSeq = new HashMap<>();
            for (DeliveryMessage message : received) {
                int seq = mapper.readTree(message.payload()).get("seq").asInt();
                int previous = lastSeq.getOrDefault(message.endpointId(), -1);
                assertThat(seq).as("per-endpoint order for %s", message.endpointId()).isEqualTo(previous + 1);
                lastSeq.put(message.endpointId(), seq);
            }
        } finally {
            relays.forEach(OutboxRelay::stop);
        }

        long unpublished = jdbc.sql("SELECT count(*) FROM outbox WHERE endpoint_id IN (:ids) AND published_at IS NULL")
                .param("ids", endpointTypes.keySet())
                .query(Long.class)
                .single();
        assertThat(unpublished).isZero();
        long relaysThatPublished = registries.stream()
                .filter(r -> r.get("outbox.published").counter().count() > 0)
                .count();
        assertThat(relaysThatPublished).as("relays that actually competed for shards").isGreaterThanOrEqualTo(2);
    }

    private void collect(KafkaConsumer<String, String> consumer, Map<UUID, String> ours, List<DeliveryMessage> into) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
            DeliveryMessage message = mapper.readValue(record.value(), DeliveryMessage.class);
            if (ours.containsKey(message.endpointId())) {
                assertThat(record.key()).isEqualTo(message.endpointId().toString());
                into.add(message);
            }
        }
    }

    private static KafkaConsumer<String, String> consumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2000);
        return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }
}
