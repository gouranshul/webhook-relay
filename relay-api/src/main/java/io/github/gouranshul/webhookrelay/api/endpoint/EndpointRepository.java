package io.github.gouranshul.webhookrelay.api.endpoint;

import io.github.gouranshul.webhookrelay.common.model.EndpointStatus;
import io.github.gouranshul.webhookrelay.common.model.OutboxShards;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class EndpointRepository {

    private static final String COLUMNS =
            "id, url, description, event_types, rate_limit_per_sec, status, consecutive_dlt, created_at, updated_at";

    private final JdbcClient jdbc;

    public EndpointRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insertSecret(UUID secretId, String keyId, byte[] ciphertext) {
        jdbc.sql("INSERT INTO endpoint_secrets (id, key_id, ciphertext) VALUES (?, ?, ?)")
                .params(secretId, keyId, ciphertext)
                .update();
    }

    public Endpoint insert(UUID id, String url, String description, List<String> eventTypes, int rateLimit,
            UUID secretRef) {
        return jdbc.sql("""
                        INSERT INTO endpoints (id, url, description, secret_ref, event_types, rate_limit_per_sec)
                        VALUES (?, ?, ?, ?, ?, ?)
                        RETURNING %s""".formatted(COLUMNS))
                .params(id, url, description, secretRef, eventTypes.toArray(String[]::new), rateLimit)
                .query(EndpointRepository::map)
                .single();
    }

    public Optional<Endpoint> find(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM endpoints WHERE id = ? AND status <> 'DELETED'")
                .param(id)
                .query(EndpointRepository::map)
                .optional();
    }

    /** Locks the row so a concurrent pause/resume or dispatcher park cannot interleave with this transaction. */
    public Optional<Endpoint> findForUpdate(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM endpoints WHERE id = ? AND status <> 'DELETED' FOR UPDATE")
                .param(id)
                .query(EndpointRepository::map)
                .optional();
    }

    public List<Endpoint> list(int limit) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM endpoints WHERE status <> 'DELETED' ORDER BY created_at DESC LIMIT ?")
                .param(limit)
                .query(EndpointRepository::map)
                .list();
    }

    public Endpoint update(UUID id, String url, String description, List<String> eventTypes, int rateLimit,
            EndpointStatus status, boolean resetDltCounter) {
        return jdbc.sql("""
                        UPDATE endpoints
                           SET url = ?, description = ?, event_types = ?, rate_limit_per_sec = ?, status = ?,
                               consecutive_dlt = CASE WHEN ? THEN 0 ELSE consecutive_dlt END, updated_at = now()
                         WHERE id = ?
                        RETURNING %s""".formatted(COLUMNS))
                .params(url, description, eventTypes.toArray(String[]::new), rateLimit, status.name(), resetDltCounter, id)
                .query(EndpointRepository::map)
                .single();
    }

    /** Re-enqueues parked deliveries through the outbox, oldest first, in the caller's transaction. */
    public int unparkDeliveries(UUID endpointId) {
        return jdbc.sql("""
                        WITH unparked AS (
                            UPDATE deliveries SET status = 'PENDING', updated_at = now()
                             WHERE endpoint_id = ? AND status = 'PARKED'
                            RETURNING id, endpoint_id, created_at
                        )
                        INSERT INTO outbox (delivery_id, endpoint_id, shard)
                        SELECT id, endpoint_id, %s FROM unparked ORDER BY created_at""".formatted(OutboxShards.SHARD_SQL))
                .param(endpointId)
                .update();
    }

    /** Soft delete. Parked deliveries are cancelled here; in-flight ones are cancelled by the dispatcher. */
    public boolean softDelete(UUID id) {
        int updated = jdbc.sql("UPDATE endpoints SET status = 'DELETED', updated_at = now() WHERE id = ? AND status <> 'DELETED'")
                .param(id)
                .update();
        if (updated > 0) {
            jdbc.sql("""
                    UPDATE deliveries SET status = 'CANCELLED', completed_at = now(), updated_at = now()
                     WHERE endpoint_id = ? AND status = 'PARKED'""")
                    .param(id)
                    .update();
        }
        return updated > 0;
    }

    static Endpoint map(ResultSet rs, int row) throws SQLException {
        Array types = rs.getArray("event_types");
        return new Endpoint(
                rs.getObject("id", UUID.class),
                rs.getString("url"),
                rs.getString("description"),
                List.copyOf(Arrays.asList((String[]) types.getArray())),
                rs.getInt("rate_limit_per_sec"),
                EndpointStatus.valueOf(rs.getString("status")),
                rs.getInt("consecutive_dlt"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }
}
