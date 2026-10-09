package io.github.gouranshul.webhookrelay.api.event;

import java.time.Duration;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Idempotency keys live in Postgres, not Redis, so the key and the event commit or roll back together. A Redis
 * key written outside the transaction could survive a rollback (event lost, retries rejected) or vanish after a
 * commit (event duplicated).
 */
@Repository
public class IdempotencyRepository {

    public record StoredResponse(String requestHash, int status, String body) {
    }

    private final JdbcClient jdbc;

    public IdempotencyRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Claims the key for this transaction. Returns true if this request owns it (new, or the previous use
     * expired). If another transaction is inserting the same key, Postgres blocks on the unique index until
     * that one commits or rolls back, so concurrent duplicates serialise here.
     */
    public boolean claim(String key, String requestHash, Duration ttl) {
        return jdbc.sql("""
                        INSERT INTO idempotency_keys (idempotency_key, request_hash, response_status, response_body, expires_at)
                        VALUES (?, ?, 0, '', now() + make_interval(secs => ?))
                        ON CONFLICT (idempotency_key) DO UPDATE
                           SET request_hash = EXCLUDED.request_hash, response_status = 0, response_body = '',
                               created_at = now(), expires_at = EXCLUDED.expires_at
                         WHERE idempotency_keys.expires_at < now()
                        RETURNING idempotency_key""")
                .params(key, requestHash, (double) ttl.toSeconds())
                .query(String.class)
                .optional()
                .isPresent();
    }

    public Optional<StoredResponse> find(String key) {
        return jdbc.sql("SELECT request_hash, response_status, response_body FROM idempotency_keys WHERE idempotency_key = ?")
                .param(key)
                .query((rs, n) -> new StoredResponse(rs.getString(1), rs.getInt(2), rs.getString(3)))
                .optional();
    }

    public void complete(String key, int status, String body) {
        jdbc.sql("UPDATE idempotency_keys SET response_status = ?, response_body = ? WHERE idempotency_key = ?")
                .params(status, body, key)
                .update();
    }

    public int purgeExpired(int limit) {
        return jdbc.sql("""
                        DELETE FROM idempotency_keys
                         WHERE idempotency_key IN (
                               SELECT idempotency_key FROM idempotency_keys WHERE expires_at < now() LIMIT ?)""")
                .param(limit)
                .update();
    }
}
