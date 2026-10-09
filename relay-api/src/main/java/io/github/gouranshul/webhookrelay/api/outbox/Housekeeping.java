package io.github.gouranshul.webhookrelay.api.outbox;

import io.github.gouranshul.webhookrelay.api.config.RelayApiProperties;
import io.github.gouranshul.webhookrelay.api.event.IdempotencyRepository;
import java.time.Duration;
import java.util.function.IntSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Keeps the database bounded: published outbox rows past their retention, expired idempotency keys, and
 * finished deliveries (with their attempts) and events older than {@code relay.retention.deliveries}.
 * Everything is deleted in bounded batches so a large backlog never turns into one long-running delete.
 * Idempotent; safe on every instance.
 */
@Component
public class Housekeeping {

    private static final Logger log = LoggerFactory.getLogger(Housekeeping.class);
    private static final int BATCH = 5_000;
    private static final int MAX_BATCHES_PER_RUN = 20;

    private final OutboxRepository outbox;
    private final IdempotencyRepository idempotency;
    private final JdbcClient jdbc;
    private final RelayApiProperties properties;

    public Housekeeping(OutboxRepository outbox, IdempotencyRepository idempotency, JdbcClient jdbc,
            RelayApiProperties properties) {
        this.outbox = outbox;
        this.idempotency = idempotency;
        this.jdbc = jdbc;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${relay.housekeeping.interval:60s}", initialDelayString = "${relay.housekeeping.interval:60s}")
    public void run() {
        try {
            int outboxRows = drain(() -> outbox.deletePublishedOlderThan(properties.outbox().retention(), BATCH));
            int keys = drain(() -> idempotency.purgeExpired(BATCH));
            Duration keep = properties.retention().deliveries();
            int deliveries = drain(() -> deleteFinishedDeliveriesOlderThan(keep));
            int events = drain(() -> deleteOrphanedEventsOlderThan(keep));
            if (outboxRows + keys + deliveries + events > 0) {
                log.info("Housekeeping removed {} outbox rows, {} idempotency keys, {} deliveries, {} events",
                        outboxRows, keys, deliveries, events);
            }
        } catch (RuntimeException e) {
            log.warn("Housekeeping run failed: {}", e.toString());
        }
    }

    /** Terminal deliveries only; their attempts and outbox rows cascade. Open deliveries are never purged. */
    int deleteFinishedDeliveriesOlderThan(Duration age) {
        return jdbc.sql("""
                        DELETE FROM deliveries WHERE id IN (
                            SELECT id FROM deliveries
                             WHERE completed_at < now() - make_interval(secs => ?)
                               AND status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
                             LIMIT ?)""")
                .params((double) age.toSeconds(), BATCH)
                .update();
    }

    int deleteOrphanedEventsOlderThan(Duration age) {
        return jdbc.sql("""
                        DELETE FROM events WHERE id IN (
                            SELECT e.id FROM events e
                             WHERE e.created_at < now() - make_interval(secs => ?)
                               AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.event_id = e.id)
                             LIMIT ?)""")
                .params((double) age.toSeconds(), BATCH)
                .update();
    }

    private static int drain(IntSupplier batch) {
        int total = 0;
        for (int i = 0; i < MAX_BATCHES_PER_RUN; i++) {
            int deleted = batch.getAsInt();
            total += deleted;
            if (deleted < BATCH) {
                break;
            }
        }
        return total;
    }
}
