package io.github.gouranshul.webhookrelay.dispatcher.delivery;

import io.github.gouranshul.webhookrelay.common.model.OutboxShards;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The safety net for at-least-once. A delivery is IN_FLIGHT under a lease while its attempt runs. If the attempt
 * never reports back (the dispatcher crashed, or forwarding to the retry topic failed because Kafka was down),
 * the lease expires and this re-enqueues the delivery through the outbox, which publishes it to the main topic
 * again. Its Kafka record was already committed, so without this the delivery would be stuck forever.
 *
 * <p>{@code FOR UPDATE SKIP LOCKED} makes it safe on every dispatcher instance at once.
 */
@Component
public class LeaseSweeper {

    private static final Logger log = LoggerFactory.getLogger(LeaseSweeper.class);
    private static final int BATCH = 500;

    private final JdbcClient jdbc;
    private final Counter requeued;

    public LeaseSweeper(JdbcClient jdbc, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.requeued = Counter.builder("webhook.delivery.lease_expired")
                .description("In-flight deliveries whose lease expired and were re-enqueued")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${relay.dispatcher.lease-sweep-interval:10s}",
            initialDelayString = "${relay.dispatcher.lease-sweep-interval:10s}")
    public void sweep() {
        try {
            int total = 0;
            int n;
            do {
                n = sweepOnce();
                total += n;
            } while (n == BATCH);
            if (total > 0) {
                requeued.increment(total);
                log.warn("Re-enqueued {} deliveries whose attempt lease expired", total);
            }
        } catch (RuntimeException e) {
            log.warn("Lease sweep failed: {}", e.toString());
        }
    }

    int sweepOnce() {
        return jdbc.sql("""
                        WITH expired AS (
                            SELECT id FROM deliveries
                             WHERE status = 'IN_FLIGHT' AND lease_until < now()
                             ORDER BY lease_until
                             LIMIT ?
                               FOR UPDATE SKIP LOCKED
                        ), requeued AS (
                            UPDATE deliveries d
                               SET status = 'RETRYING', lease_until = NULL, updated_at = now(),
                                   last_error = COALESCE(d.last_error, 'attempt lease expired; re-enqueued')
                              FROM expired WHERE d.id = expired.id
                            RETURNING d.id, d.endpoint_id
                        )
                        INSERT INTO outbox (delivery_id, endpoint_id, shard)
                        SELECT id, endpoint_id, %s FROM requeued""".formatted(OutboxShards.SHARD_SQL))
                .param(BATCH)
                .update();
    }
}
