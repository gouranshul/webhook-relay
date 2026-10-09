package io.github.gouranshul.webhookrelay.api.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * {@code outbox.pending} (unpublished rows) and {@code outbox.lag.seconds} (age of the oldest one).
 *
 * <p>Sampled on a schedule rather than on scrape, so a burst of Prometheus scrapes cannot turn into a burst of
 * count queries. The values are global (every API instance reports the same backlog), so dashboards should
 * aggregate with {@code max}, not {@code sum}.
 */
@Component
public class OutboxMetrics {

    private static final Logger log = LoggerFactory.getLogger(OutboxMetrics.class);

    private final OutboxRepository repository;
    private volatile OutboxRepository.Backlog backlog = new OutboxRepository.Backlog(0, 0);

    public OutboxMetrics(OutboxRepository repository, MeterRegistry registry) {
        this.repository = repository;
        Gauge.builder("outbox.pending", this, m -> m.backlog.pending())
                .description("Outbox rows not yet published to Kafka")
                .register(registry);
        Gauge.builder("outbox.lag.seconds", this, m -> m.backlog.oldestAgeSeconds())
                .description("Age of the oldest unpublished outbox row")
                .baseUnit("seconds")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${relay.outbox.metrics-interval:5s}")
    public void sample() {
        try {
            backlog = repository.backlog();
        } catch (RuntimeException e) {
            log.debug("Could not sample outbox backlog: {}", e.toString());
        }
    }
}
