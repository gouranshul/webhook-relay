package io.github.gouranshul.webhookrelay.dispatcher.endpoint;

import io.github.gouranshul.webhookrelay.common.Topics;
import io.github.gouranshul.webhookrelay.dispatcher.config.DispatcherProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Auto-pauses an endpoint after {@code relay.auto-pause.consecutive-dlt} deliveries in a row reach the DLT
 * (any success resets the count), and emits an {@code endpoint.paused} event on {@code webhook.system-events}.
 *
 * <p>Pausing parks further deliveries instead of letting them run through the retry chain into the DLT, so an
 * endpoint that has been gone for days stops costing retries and stops filling the DLT. Resuming it via
 * {@code PATCH /v1/endpoints/{id}} re-enqueues everything that was parked.
 */
@Component
public class EndpointHealth {

    private static final Logger log = LoggerFactory.getLogger(EndpointHealth.class);

    private final JdbcClient jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final EndpointCache cache;
    private final Clock clock;
    private final int threshold;
    private final Counter autoPaused;

    public EndpointHealth(JdbcClient jdbc, KafkaTemplate<String, String> kafka, ObjectMapper mapper,
            EndpointCache cache, Clock clock, DispatcherProperties properties, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.mapper = mapper;
        this.cache = cache;
        this.clock = clock;
        this.threshold = properties.autoPause().consecutiveDlt();
        this.autoPaused = Counter.builder("webhook.endpoint.auto_paused")
                .description("Endpoints paused automatically after consecutive DLT entries")
                .register(registry);
    }

    public void onDeadLettered(UUID endpointId) {
        Integer consecutive = jdbc.sql("""
                        UPDATE endpoints SET consecutive_dlt = consecutive_dlt + 1
                         WHERE id = ? RETURNING consecutive_dlt""")
                .param(endpointId)
                .query(Integer.class)
                .optional()
                .orElse(0);
        if (threshold <= 0 || consecutive < threshold) {
            return;
        }
        // Only the transition ACTIVE -> PAUSED emits, so concurrent DLT handlers produce one event.
        int paused = jdbc.sql("UPDATE endpoints SET status = 'PAUSED', updated_at = now() WHERE id = ? AND status = 'ACTIVE'")
                .param(endpointId)
                .update();
        if (paused == 0) {
            return;
        }
        cache.invalidate(endpointId);
        autoPaused.increment();
        log.warn("Endpoint {} auto-paused after {} consecutive dead-lettered deliveries", endpointId, consecutive);
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("type", "endpoint.paused");
        event.put("endpointId", endpointId.toString());
        event.put("reason", "consecutive_dlt");
        event.put("consecutiveDlt", consecutive);
        event.put("at", clock.instant().toString());
        kafka.send(Topics.SYSTEM_EVENTS, endpointId.toString(), mapper.writeValueAsString(event))
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.warn("Could not emit endpoint.paused for {}: {}", endpointId, error.toString());
                    }
                });
    }
}
