package io.github.gouranshul.webhookrelay.api.event;

import io.github.gouranshul.webhookrelay.common.model.OutboxShards;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class EventRepository {

    private final JdbcClient jdbc;

    public EventRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Instant insert(UUID id, String type, String payloadJson, String traceparent) {
        return jdbc.sql("""
                        INSERT INTO events (id, event_type, payload, traceparent)
                        VALUES (?, ?, CAST(? AS jsonb), ?)
                        RETURNING created_at""")
                .params(id, type, payloadJson, traceparent)
                .query(Timestamp.class)
                .single()
                .toInstant();
    }

    /**
     * Fan-out: one delivery and one outbox row per endpoint subscribed to the type (or to {@code *}), in the
     * caller's transaction. Paused endpoints get rows too; the dispatcher parks them, so nothing is dropped if
     * the endpoint is resumed concurrently.
     *
     * @return number of deliveries created
     */
    public int fanOut(UUID eventId, String type) {
        return jdbc.sql("""
                        WITH targets AS (
                            SELECT gen_random_uuid() AS delivery_id, id AS endpoint_id
                              FROM endpoints
                             WHERE status <> 'DELETED' AND event_types && ARRAY[?, '*']::text[]
                        ), created AS (
                            INSERT INTO deliveries (id, webhook_id, event_id, endpoint_id, status)
                            SELECT delivery_id, delivery_id, ?, endpoint_id, 'PENDING' FROM targets
                            RETURNING id, endpoint_id
                        )
                        INSERT INTO outbox (delivery_id, endpoint_id, shard)
                        SELECT id, endpoint_id, %s FROM created""".formatted(OutboxShards.SHARD_SQL))
                .params(type, eventId)
                .update();
    }
}
