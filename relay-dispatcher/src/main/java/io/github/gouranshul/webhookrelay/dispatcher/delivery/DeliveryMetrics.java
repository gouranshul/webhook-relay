package io.github.gouranshul.webhookrelay.dispatcher.delivery;

import io.github.gouranshul.webhookrelay.dispatcher.http.HttpOutcome;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import org.springframework.stereotype.Component;

/**
 * Delivery metrics. Tags are low-cardinality on purpose: outcome and attempt number (capped), never endpoint
 * id, so a tenant with 100k endpoints cannot blow up the metrics backend.
 */
@Component
public class DeliveryMetrics {

    private final MeterRegistry registry;
    private final Counter deadLettered;

    public DeliveryMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.deadLettered = Counter.builder("webhook.delivery.dlt")
                .description("Deliveries that reached the dead-letter topic")
                .register(registry);
    }

    public void attempt(int attempt, HttpOutcome outcome) {
        String kind = outcome.kind().name().toLowerCase();
        Timer.builder("webhook.delivery.duration")
                .description("Latency of webhook HTTP attempts")
                .tag("outcome", kind)
                .publishPercentileHistogram()
                .serviceLevelObjectives(Duration.ofMillis(100), Duration.ofMillis(250), Duration.ofMillis(500),
                        Duration.ofSeconds(1), Duration.ofSeconds(5))
                .register(registry)
                .record(outcome.latency());
        Counter.builder("webhook.delivery.attempts")
                .description("Webhook HTTP attempts by attempt number and outcome")
                .tag("attempt", attempt > 6 ? "7+" : Integer.toString(attempt))
                .tag("outcome", kind)
                .register(registry)
                .increment();
    }

    /** An attempt that was not made: circuit open, bulkhead full, rate limited, endpoint paused or deleted. */
    public void skipped(String reason) {
        Counter.builder("webhook.delivery.skipped")
                .description("Records handled without an HTTP attempt")
                .tag("reason", reason)
                .register(registry)
                .increment();
    }

    public void deadLettered() {
        deadLettered.increment();
    }
}
