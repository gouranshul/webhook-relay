package io.github.gouranshul.webhookrelay.api.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gouranshul.webhookrelay.api.ApiIntegrationTest;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class HousekeepingTest extends ApiIntegrationTest {

    @Autowired
    Housekeeping housekeeping;

    private UUID deliveryOf(UUID eventId) {
        return jdbc.sql("SELECT id FROM deliveries WHERE event_id = ?").param(eventId).query(UUID.class).single();
    }

    private UUID publish(String type) {
        return UUID.fromString(json(post("/v1/events", "{\"type\":\"%s\",\"data\":{}}".formatted(type))).get("id").asString());
    }

    @Test
    void purgesOldFinishedDeliveriesWithAttemptsButNeverOpenOnes() {
        String type = uniqueType("order");
        createEndpoint(List.of(type));
        UUID oldEvent = publish(type);
        UUID openEvent = publish(type);
        UUID old = deliveryOf(oldEvent);
        UUID open = deliveryOf(openEvent);
        jdbc.sql("UPDATE deliveries SET status = 'SUCCEEDED', completed_at = now() - interval '40 days' WHERE id = ?").param(old).update();
        jdbc.sql("UPDATE deliveries SET status = 'RETRYING' WHERE id = ?").param(open).update();
        jdbc.sql("UPDATE events SET created_at = now() - interval '40 days' WHERE id IN (?, ?)").params(oldEvent, openEvent).update();
        jdbc.sql("INSERT INTO delivery_attempts (delivery_id, attempt_number, outcome, status_code, latency_ms) VALUES (?, 1, 'SUCCESS', 200, 10)")
                .param(old).update();
        // A replay of the old delivery must survive the purge of its original.
        UUID replay = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO deliveries (id, webhook_id, event_id, endpoint_id, replay_of, status)
                SELECT ?, webhook_id, event_id, endpoint_id, id, 'PENDING' FROM deliveries WHERE id = ?""")
                .params(replay, old).update();

        housekeeping.deleteFinishedDeliveriesOlderThan(Duration.ofDays(30));
        housekeeping.deleteOrphanedEventsOlderThan(Duration.ofDays(30));

        assertThat(count("SELECT count(*) FROM deliveries WHERE id = ?", old)).isZero();
        assertThat(count("SELECT count(*) FROM delivery_attempts WHERE delivery_id = ?", old)).isZero();
        assertThat(count("SELECT count(*) FROM deliveries WHERE id = ?", open)).isEqualTo(1);
        assertThat(jdbc.sql("SELECT replay_of FROM deliveries WHERE id = ?").param(replay).query(UUID.class).optional())
                .isEmpty();
        // The old event still has its replay delivery, so it is kept; the open one too.
        assertThat(count("SELECT count(*) FROM events WHERE id = ?", oldEvent)).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM events WHERE id = ?", openEvent)).isEqualTo(1);

        jdbc.sql("DELETE FROM deliveries WHERE id = ?").param(replay).update();
        housekeeping.deleteOrphanedEventsOlderThan(Duration.ofDays(30));
        assertThat(count("SELECT count(*) FROM events WHERE id = ?", oldEvent)).isZero();
    }

    private long count(String sql, UUID id) {
        return jdbc.sql(sql).param(id).query(Long.class).single();
    }
}
