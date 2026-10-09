package io.github.gouranshul.webhookrelay.dispatcher.delivery;

import io.github.gouranshul.webhookrelay.common.model.DeliveryStatus;
import io.github.gouranshul.webhookrelay.dispatcher.http.HttpOutcome;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * Delivery state transitions. The {@code deliveries} row is the source of truth that makes the consumer
 * idempotent: a Kafka record is only acted on if it can claim the row.
 */
@Repository
public class DeliveryRepository {

    private final JdbcClient jdbc;

    public DeliveryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Atomically claims the delivery for one attempt: PENDING/RETRYING, or IN_FLIGHT with an expired lease
     * (its worker died), becomes IN_FLIGHT with a fresh lease. Returns the new attempt number, or empty if
     * the row is terminal, parked, or leased by another worker.
     */
    public OptionalInt claim(UUID deliveryId, Duration lease) {
        return jdbc.sql("""
                        UPDATE deliveries
                           SET status = 'IN_FLIGHT', lease_until = now() + make_interval(secs => ?),
                               attempt_count = attempt_count + 1, updated_at = now()
                         WHERE id = ?
                           AND (status IN ('PENDING', 'RETRYING') OR (status = 'IN_FLIGHT' AND lease_until < now()))
                        RETURNING attempt_count""")
                .params(lease.toMillis() / 1000.0, deliveryId)
                .query(Integer.class)
                .optional()
                .map(OptionalInt::of)
                .orElse(OptionalInt.empty());
    }

    /** Current status and, while IN_FLIGHT, how long the lease has left. */
    public record State(DeliveryStatus status, Duration leaseRemaining) {
    }

    public Optional<State> state(UUID deliveryId) {
        return jdbc.sql("""
                        SELECT status, GREATEST(0, EXTRACT(EPOCH FROM (lease_until - now())) * 1000)::bigint AS lease_ms
                          FROM deliveries WHERE id = ?""")
                .param(deliveryId)
                .query((rs, n) -> new State(DeliveryStatus.valueOf(rs.getString("status")),
                        Duration.ofMillis(rs.getLong("lease_ms"))))
                .optional();
    }

    /** Records the attempt and moves the delivery to its next state, in one transaction. */
    @Transactional
    public void recordAttempt(UUID deliveryId, UUID endpointId, int attempt, HttpOutcome outcome) {
        jdbc.sql("""
                        INSERT INTO delivery_attempts (delivery_id, attempt_number, outcome, status_code, latency_ms, error)
                        VALUES (?, ?, ?, ?, ?, ?)""")
                .params(deliveryId, attempt, outcome.kind().name(), outcome.statusCode(),
                        (int) Math.min(Integer.MAX_VALUE, outcome.latency().toMillis()), outcome.error())
                .update();
        switch (outcome.kind()) {
            case SUCCESS -> {
                // Success wins over everything except an earlier success: a late in-flight success after the
                // DLT handler gave up still reflects what the receiver actually got.
                jdbc.sql("""
                                UPDATE deliveries
                                   SET status = 'SUCCEEDED', last_status_code = ?, last_error = NULL, lease_until = NULL,
                                       completed_at = now(), updated_at = now()
                                 WHERE id = ? AND status <> 'SUCCEEDED'""")
                        .params(outcome.statusCode(), deliveryId)
                        .update();
                jdbc.sql("UPDATE endpoints SET consecutive_dlt = 0 WHERE id = ? AND consecutive_dlt > 0")
                        .param(endpointId)
                        .update();
            }
            case PERMANENT -> jdbc.sql("""
                            UPDATE deliveries
                               SET status = 'FAILED', last_status_code = ?, last_error = ?, lease_until = NULL,
                                   completed_at = now(), updated_at = now()
                             WHERE id = ? AND status = 'IN_FLIGHT'""")
                    .params(outcome.statusCode(), outcome.error(), deliveryId)
                    .update();
            default -> jdbc.sql("""
                            UPDATE deliveries
                               SET status = 'RETRYING', last_status_code = ?, last_error = ?, lease_until = NULL,
                                   updated_at = now()
                             WHERE id = ? AND status = 'IN_FLIGHT'""")
                    .params(outcome.statusCode(), outcome.error(), deliveryId)
                    .update();
        }
    }

    /** Releases a claim without an HTTP attempt (e.g. the call was deferred), undoing the attempt count. */
    public void release(UUID deliveryId, String reason) {
        jdbc.sql("""
                        UPDATE deliveries
                           SET status = 'RETRYING', attempt_count = attempt_count - 1, last_error = ?,
                               lease_until = NULL, updated_at = now()
                         WHERE id = ? AND status = 'IN_FLIGHT'""")
                .params(reason, deliveryId)
                .update();
    }

    /**
     * Parks the delivery if, and only if, the endpoint is paused right now. The endpoint row is share-locked,
     * so this cannot interleave with a concurrent resume (which locks it FOR UPDATE and then un-parks): either
     * the resume sees this parked row, or this statement sees the endpoint already active.
     */
    @Transactional
    public boolean parkIfEndpointPaused(UUID deliveryId, UUID endpointId) {
        Boolean paused = jdbc.sql("SELECT status = 'PAUSED' FROM endpoints WHERE id = ? FOR SHARE")
                .param(endpointId)
                .query(Boolean.class)
                .optional()
                .orElse(false);
        if (!paused) {
            return false;
        }
        jdbc.sql("""
                        UPDATE deliveries SET status = 'PARKED', lease_until = NULL, updated_at = now()
                         WHERE id = ? AND status IN ('PENDING', 'RETRYING', 'IN_FLIGHT')""")
                .param(deliveryId)
                .update();
        return true;
    }

    public void cancel(UUID deliveryId) {
        jdbc.sql("""
                        UPDATE deliveries SET status = 'CANCELLED', lease_until = NULL, completed_at = now(), updated_at = now()
                         WHERE id = ? AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')""")
                .param(deliveryId)
                .update();
    }

    /**
     * Called by the DLT handler. Returns true exactly once per delivery (guarded by {@code dlt_at}), so the
     * endpoint's consecutive-DLT counter is not inflated if the DLT record is redelivered.
     */
    public boolean markDeadLettered(UUID deliveryId, String reason) {
        return jdbc.sql("""
                        UPDATE deliveries
                           SET status = 'FAILED', dlt_at = now(), lease_until = NULL,
                               last_error = COALESCE(last_error, ?), completed_at = COALESCE(completed_at, now()),
                               updated_at = now()
                         WHERE id = ? AND dlt_at IS NULL AND status NOT IN ('SUCCEEDED', 'CANCELLED')""")
                .params(reason, deliveryId)
                .update() > 0;
    }
}
