package io.github.gouranshul.webhookrelay.api.event;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gouranshul.webhookrelay.api.ApiIntegrationTest;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

/**
 * The event, its deliveries, its outbox rows and its idempotency key commit together or not at all. A database
 * trigger makes the outbox insert fail for one poisoned event type, as a real constraint violation or a crash
 * mid-transaction would; afterwards nothing from that request may be visible.
 */
class AtomicityTest extends ApiIntegrationTest {

    private final String poisonType = uniqueType("poison");

    @BeforeEach
    void installTrigger() {
        jdbc.sql("""
                CREATE OR REPLACE FUNCTION fail_outbox_for_poison() RETURNS trigger AS $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM deliveries d JOIN events e ON e.id = d.event_id
                                WHERE d.id = NEW.delivery_id AND e.event_type LIKE 'poison.%') THEN
                        RAISE EXCEPTION 'simulated outbox failure';
                    END IF;
                    RETURN NEW;
                END $$ LANGUAGE plpgsql""").update();
        jdbc.sql("""
                CREATE OR REPLACE TRIGGER outbox_poison BEFORE INSERT ON outbox
                FOR EACH ROW EXECUTE FUNCTION fail_outbox_for_poison()""").update();
    }

    @AfterEach
    void dropTrigger() {
        jdbc.sql("DROP TRIGGER IF EXISTS outbox_poison ON outbox").update();
    }

    @Test
    void failureWritingOutboxRollsBackEverything() {
        UUID endpoint = createEndpoint(List.of(poisonType));
        String key = "key-" + UUID.randomUUID();

        var result = post("/v1/events", "{\"type\":\"%s\",\"data\":{\"x\":1}}".formatted(poisonType),
                "Idempotency-Key", key);

        assertThat(result).hasStatus(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(count("SELECT count(*) FROM events WHERE event_type = ?", poisonType)).isZero();
        assertThat(count("SELECT count(*) FROM deliveries WHERE endpoint_id = ?", endpoint)).isZero();
        assertThat(count("SELECT count(*) FROM outbox WHERE endpoint_id = ?", endpoint)).isZero();
        assertThat(count("SELECT count(*) FROM idempotency_keys WHERE idempotency_key = ?", key)).isZero();
    }

    @Test
    void retryWithSameKeySucceedsOnceTheFaultIsGone() {
        UUID endpoint = createEndpoint(List.of(poisonType));
        String key = "key-" + UUID.randomUUID();
        String body = "{\"type\":\"%s\",\"data\":{\"x\":1}}".formatted(poisonType);
        assertThat(post("/v1/events", body, "Idempotency-Key", key)).hasStatus(HttpStatus.INTERNAL_SERVER_ERROR);

        dropTrigger();

        assertThat(post("/v1/events", body, "Idempotency-Key", key)).hasStatus(HttpStatus.ACCEPTED);
        assertThat(count("SELECT count(*) FROM events WHERE event_type = ?", poisonType)).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM outbox WHERE endpoint_id = ? AND published_at IS NULL", endpoint))
                .isEqualTo(1);
    }

    private long count(String sql, Object param) {
        return jdbc.sql(sql).param(param).query(Long.class).single();
    }
}
