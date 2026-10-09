package io.github.gouranshul.webhookrelay.api.delivery;

import io.github.gouranshul.webhookrelay.api.delivery.DeliveryDtos.AttemptView;
import io.github.gouranshul.webhookrelay.api.delivery.DeliveryDtos.DeliverySummary;
import io.github.gouranshul.webhookrelay.common.model.DeliveryStatus;
import io.github.gouranshul.webhookrelay.common.model.OutboxShards;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class DeliveryQueryRepository {

    private static final String SUMMARY = """
            SELECT d.id, d.webhook_id, d.event_id, e.event_type, d.endpoint_id, d.status, d.attempt_count,
                   d.last_status_code, d.last_error, d.replay_of, d.created_at, d.updated_at, d.completed_at, d.dlt_at
              FROM deliveries d JOIN events e ON e.id = d.event_id""";

    private final JdbcClient jdbc;

    public DeliveryQueryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Keyset page, newest first. {@code after} is the (created_at, id) of the last row of the previous page. */
    public List<DeliverySummary> list(UUID endpointId, DeliveryStatus status, UUID eventId, Instant afterCreatedAt,
            UUID afterId, int limit) {
        StringBuilder sql = new StringBuilder(SUMMARY).append(" WHERE true");
        Map<String, Object> params = new LinkedHashMap<>();
        if (endpointId != null) {
            sql.append(" AND d.endpoint_id = :endpointId");
            params.put("endpointId", endpointId);
        }
        if (status != null) {
            sql.append(" AND d.status = :status");
            params.put("status", status.name());
        }
        if (eventId != null) {
            sql.append(" AND d.event_id = :eventId");
            params.put("eventId", eventId);
        }
        if (afterCreatedAt != null) {
            sql.append(" AND (d.created_at, d.id) < (:afterCreatedAt, :afterId)");
            params.put("afterCreatedAt", Timestamp.from(afterCreatedAt));
            params.put("afterId", afterId);
        }
        sql.append(" ORDER BY d.created_at DESC, d.id DESC LIMIT :limit");
        params.put("limit", limit);
        return jdbc.sql(sql.toString()).params(params).query(DeliveryQueryRepository::summary).list();
    }

    public Optional<DeliverySummary> find(UUID id) {
        return jdbc.sql(SUMMARY + " WHERE d.id = ?").param(id).query(DeliveryQueryRepository::summary).optional();
    }

    public List<AttemptView> attempts(UUID deliveryId) {
        return jdbc.sql("""
                        SELECT attempt_number, outcome, status_code, latency_ms, error, attempted_at
                          FROM delivery_attempts WHERE delivery_id = ? ORDER BY attempt_number, id""")
                .param(deliveryId)
                .query((rs, n) -> new AttemptView(rs.getInt(1), rs.getString(2), (Integer) rs.getObject(3),
                        rs.getInt(4), rs.getString(5), rs.getTimestamp(6).toInstant()))
                .list();
    }

    public List<UUID> replaysOf(UUID deliveryId) {
        return jdbc.sql("SELECT id FROM deliveries WHERE replay_of = ? ORDER BY created_at")
                .param(deliveryId).query(UUID.class).list();
    }

    /**
     * Creates a replay of a terminal delivery: a new PENDING delivery with the SAME webhook id (so receivers
     * that dedupe are protected) and an outbox row, in the caller's transaction.
     */
    public Optional<UUID> replay(UUID deliveryId) {
        return jdbc.sql("""
                        WITH src AS (
                            SELECT d.id, d.webhook_id, d.event_id, d.endpoint_id
                              FROM deliveries d JOIN endpoints e ON e.id = d.endpoint_id
                             WHERE d.id = ? AND d.status IN ('SUCCEEDED', 'FAILED') AND e.status <> 'DELETED'
                        ), created AS (
                            INSERT INTO deliveries (id, webhook_id, event_id, endpoint_id, replay_of, status)
                            SELECT gen_random_uuid(), webhook_id, event_id, endpoint_id, id, 'PENDING' FROM src
                            RETURNING id, endpoint_id
                        ), queued AS (
                            INSERT INTO outbox (delivery_id, endpoint_id, shard)
                            SELECT id, endpoint_id, %s FROM created
                        )
                        SELECT id FROM created""".formatted(OutboxShards.SHARD_SQL))
                .param(deliveryId)
                .query(UUID.class)
                .optional();
    }

    /**
     * Replays dead-lettered deliveries of one endpoint, oldest first, skipping any that were already replayed.
     * Serialised per endpoint with an advisory lock so two concurrent bulk replays cannot both replay a row.
     */
    public List<UUID> bulkReplay(UUID endpointId, Instant from, Instant to, int limit) {
        jdbc.sql("SELECT pg_advisory_xact_lock(hashtext('replay:' || ?::text))").param(endpointId)
                .query((rs, n) -> null).list();
        List<UUID> created = new ArrayList<>(jdbc.sql("""
                        WITH src AS (
                            SELECT d.id, d.webhook_id, d.event_id, d.endpoint_id,
                                   row_number() OVER (ORDER BY d.created_at, d.id) AS rn
                              FROM deliveries d
                             WHERE d.endpoint_id = ? AND d.status = 'FAILED' AND d.dlt_at IS NOT NULL
                               AND d.dlt_at >= ? AND d.dlt_at < ?
                               AND NOT EXISTS (SELECT 1 FROM deliveries r WHERE r.replay_of = d.id)
                             ORDER BY d.created_at, d.id
                             LIMIT ?
                        ), created AS (
                            INSERT INTO deliveries (id, webhook_id, event_id, endpoint_id, replay_of, status)
                            SELECT gen_random_uuid(), webhook_id, event_id, endpoint_id, id, 'PENDING' FROM src
                            RETURNING id, endpoint_id, replay_of
                        ), queued AS (
                            INSERT INTO outbox (delivery_id, endpoint_id, shard)
                            SELECT c.id, c.endpoint_id, %s
                              FROM created c JOIN src ON src.id = c.replay_of
                             ORDER BY src.rn
                        )
                        SELECT c.id FROM created c JOIN src ON src.id = c.replay_of ORDER BY src.rn"""
                        .formatted(OutboxShards.SHARD_SQL.replace("endpoint_id", "c.endpoint_id")))
                .params(endpointId, Timestamp.from(from), Timestamp.from(to), limit)
                .query(UUID.class)
                .list());
        return created;
    }

    public boolean endpointExists(UUID endpointId) {
        return jdbc.sql("SELECT EXISTS (SELECT 1 FROM endpoints WHERE id = ? AND status <> 'DELETED')")
                .param(endpointId).query(Boolean.class).single();
    }

    static DeliverySummary summary(ResultSet rs, int row) throws SQLException {
        return new DeliverySummary(
                rs.getObject("id", UUID.class),
                rs.getObject("webhook_id", UUID.class),
                rs.getObject("event_id", UUID.class),
                rs.getString("event_type"),
                rs.getObject("endpoint_id", UUID.class),
                DeliveryStatus.valueOf(rs.getString("status")),
                rs.getInt("attempt_count"),
                (Integer) rs.getObject("last_status_code"),
                rs.getString("last_error"),
                rs.getObject("replay_of", UUID.class),
                instant(rs, "created_at"),
                instant(rs, "updated_at"),
                instant(rs, "completed_at"),
                instant(rs, "dlt_at"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp ts = rs.getTimestamp(column);
        return ts == null ? null : ts.toInstant();
    }
}
