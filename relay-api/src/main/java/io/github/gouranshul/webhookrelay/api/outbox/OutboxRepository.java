package io.github.gouranshul.webhookrelay.api.outbox;

import io.github.gouranshul.webhookrelay.common.model.DeliveryMessage;
import io.github.gouranshul.webhookrelay.common.model.OutboxShards;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class OutboxRepository {

    public record PendingRow(long id, DeliveryMessage message, String traceparent) {
    }

    /** Snapshot for the {@code outbox.pending} and {@code outbox.lag.seconds} gauges. */
    public record Backlog(long pending, double oldestAgeSeconds) {
    }

    private final JdbcClient jdbc;

    public OutboxRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public boolean hasPending() {
        return jdbc.sql("SELECT EXISTS (SELECT 1 FROM outbox WHERE published_at IS NULL)")
                .query(Boolean.class)
                .single();
    }

    /**
     * Takes the shard's transaction-scoped advisory lock without waiting. Only one relay (across all
     * instances) publishes a shard at a time, which keeps per-endpoint publish order equal to outbox order.
     */
    public boolean tryLockShard(int shard) {
        return jdbc.sql("SELECT pg_try_advisory_xact_lock(?, ?)")
                .params(OutboxShards.ADVISORY_LOCK_NAMESPACE, shard)
                .query(Boolean.class)
                .single();
    }

    /**
     * Claims the oldest unpublished rows of a shard. {@code SKIP LOCKED} is defence in depth: the advisory lock
     * already excludes other relays, and it also keeps manual tooling from blocking the relay.
     */
    public List<PendingRow> lockBatch(int shard, int limit) {
        return jdbc.sql("""
                        SELECT o.id, o.delivery_id, d.webhook_id, d.event_id, o.endpoint_id,
                               e.event_type, e.payload::text AS payload, e.created_at, e.traceparent
                          FROM outbox o
                          JOIN deliveries d ON d.id = o.delivery_id
                          JOIN events e ON e.id = d.event_id
                         WHERE o.shard = ? AND o.published_at IS NULL
                         ORDER BY o.id
                         LIMIT ?
                           FOR UPDATE OF o SKIP LOCKED""")
                .params(shard, limit)
                .query((rs, n) -> new PendingRow(
                        rs.getLong("id"),
                        new DeliveryMessage(
                                rs.getObject("delivery_id", UUID.class),
                                rs.getObject("webhook_id", UUID.class),
                                rs.getObject("event_id", UUID.class),
                                rs.getObject("endpoint_id", UUID.class),
                                rs.getString("event_type"),
                                rs.getString("payload"),
                                rs.getTimestamp("created_at").toInstant()),
                        rs.getString("traceparent")))
                .list();
    }

    public int markPublished(List<Long> ids) {
        return jdbc.sql("UPDATE outbox SET published_at = now() WHERE id IN (:ids)")
                .param("ids", ids)
                .update();
    }

    public Backlog backlog() {
        return jdbc.sql("""
                        SELECT count(*) AS pending,
                               COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0) AS age
                          FROM outbox WHERE published_at IS NULL""")
                .query((rs, n) -> new Backlog(rs.getLong("pending"), rs.getDouble("age")))
                .single();
    }

    public int deletePublishedOlderThan(Duration retention, int limit) {
        return jdbc.sql("""
                        DELETE FROM outbox WHERE id IN (
                            SELECT id FROM outbox
                             WHERE published_at IS NOT NULL AND published_at < now() - make_interval(secs => ?)
                             LIMIT ?)""")
                .params((double) retention.toSeconds(), limit)
                .update();
    }
}
