package io.github.gouranshul.webhookrelay.api.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gouranshul.webhookrelay.api.ApiIntegrationTest;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.assertj.MvcTestResult;
import tools.jackson.databind.JsonNode;

/**
 * The relay and dispatcher are not running here; delivery states are set directly, as the dispatcher would.
 */
class DeliveryApiTest extends ApiIntegrationTest {

    private UUID publish(String type) {
        return UUID.fromString(json(post("/v1/events", "{\"type\":\"%s\",\"data\":{\"k\":1}}".formatted(type)))
                .get("id").asString());
    }

    private UUID deliveryOf(UUID eventId) {
        return jdbc.sql("SELECT id FROM deliveries WHERE event_id = ? AND replay_of IS NULL")
                .param(eventId).query(UUID.class).single();
    }

    private void setState(UUID deliveryId, String status, Instant dltAt) {
        jdbc.sql("UPDATE deliveries SET status = ?, dlt_at = ?, attempt_count = 2 WHERE id = ?")
                .params(status, dltAt == null ? null : java.sql.Timestamp.from(dltAt), deliveryId).update();
    }

    @Test
    void listsWithFiltersAndCursorPagination() {
        String type = uniqueType("order");
        UUID endpoint = createEndpoint(List.of(type));
        for (int i = 0; i < 5; i++) {
            publish(type);
        }
        UUID failedEvent = publish(type);
        setState(deliveryOf(failedEvent), "FAILED", Instant.now());

        JsonNode page1 = json(get("/v1/deliveries?endpointId=" + endpoint + "&limit=4"));
        assertThat(page1.get("data")).hasSize(4);
        String cursor = page1.get("nextCursor").asString();
        JsonNode page2 = json(get("/v1/deliveries?endpointId=" + endpoint + "&limit=4&cursor=" + cursor));
        assertThat(page2.get("data")).hasSize(2);
        assertThat(page2.get("nextCursor").isNull()).isTrue();

        Set<String> ids = new HashSet<>();
        page1.get("data").forEach(d -> ids.add(d.get("id").asString()));
        page2.get("data").forEach(d -> ids.add(d.get("id").asString()));
        assertThat(ids).hasSize(6);

        JsonNode failed = json(get("/v1/deliveries?endpointId=" + endpoint + "&status=FAILED"));
        assertThat(failed.get("data")).singleElement()
                .satisfies(d -> assertThat(d.get("eventId").asString()).isEqualTo(failedEvent.toString()));

        assertThat(get("/v1/deliveries?status=NOPE")).hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(get("/v1/deliveries?cursor=garbage")).hasStatus(HttpStatus.BAD_REQUEST);
    }

    @Test
    void showsAttemptsAndEventView() {
        String type = uniqueType("user");
        createEndpoint(List.of(type));
        UUID event = publish(type);
        UUID delivery = deliveryOf(event);
        jdbc.sql("""
                INSERT INTO delivery_attempts (delivery_id, attempt_number, outcome, status_code, latency_ms, error)
                VALUES (?, 1, 'RETRYABLE', 503, 120, 'HTTP 503: down'), (?, 2, 'SUCCESS', 200, 80, NULL)""")
                .params(delivery, delivery).update();
        setState(delivery, "SUCCEEDED", null);

        JsonNode detail = json(get("/v1/deliveries/" + delivery));
        assertThat(detail.get("delivery").get("status").asString()).isEqualTo("SUCCEEDED");
        assertThat(detail.get("attempts")).hasSize(2);
        assertThat(detail.get("attempts").get(0).get("statusCode").asInt()).isEqualTo(503);
        assertThat(detail.get("attempts").get(1).get("latencyMs").asInt()).isEqualTo(80);

        JsonNode eventView = json(get("/v1/events/" + event));
        assertThat(eventView.get("data").get("k").asInt()).isEqualTo(1);
        assertThat(eventView.get("deliveries")).hasSize(1);
        assertThat(get("/v1/deliveries/" + UUID.randomUUID())).hasStatus(HttpStatus.NOT_FOUND);
        assertThat(get("/v1/events/" + UUID.randomUUID())).hasStatus(HttpStatus.NOT_FOUND);
    }

    @Test
    void replayCreatesNewDeliveryWithSameWebhookIdAndOutboxRow() {
        String type = uniqueType("invoice");
        UUID endpoint = createEndpoint(List.of(type));
        UUID original = deliveryOf(publish(type));

        assertThat(post("/v1/deliveries/" + original + "/replay", "")).hasStatus(HttpStatus.CONFLICT);

        setState(original, "FAILED", Instant.now());
        MvcTestResult replay = post("/v1/deliveries/" + original + "/replay", "");
        assertThat(replay).hasStatus(HttpStatus.ACCEPTED);
        JsonNode created = json(replay);
        UUID replayId = UUID.fromString(created.get("id").asString());
        assertThat(replayId).isNotEqualTo(original);
        assertThat(created.get("replayOf").asString()).isEqualTo(original.toString());
        assertThat(created.get("status").asString()).isEqualTo("PENDING");
        assertThat(created.get("webhookId").asString()).isEqualTo(original.toString());
        assertThat(jdbc.sql("SELECT count(*) FROM outbox WHERE delivery_id = ?").param(replayId)
                .query(Long.class).single()).isEqualTo(1);
        assertThat(json(get("/v1/deliveries/" + original)).get("replays")).hasSize(1);

        delete("/v1/endpoints/" + endpoint);
        assertThat(post("/v1/deliveries/" + original + "/replay", "")).hasStatus(HttpStatus.CONFLICT);
    }

    @Test
    void bulkReplayTakesDeadLettersInRangeOnce() {
        String type = uniqueType("order");
        UUID endpoint = createEndpoint(List.of(type));
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        UUID inRange1 = deliveryOf(publish(type));
        UUID inRange2 = deliveryOf(publish(type));
        UUID tooOld = deliveryOf(publish(type));
        UUID succeeded = deliveryOf(publish(type));
        setState(inRange1, "FAILED", now.minusSeconds(60));
        setState(inRange2, "FAILED", now.minusSeconds(30));
        setState(tooOld, "FAILED", now.minus(2, ChronoUnit.DAYS));
        setState(succeeded, "SUCCEEDED", null);

        String body = "{\"endpointId\":\"%s\",\"from\":\"%s\",\"to\":\"%s\"}"
                .formatted(endpoint, now.minus(1, ChronoUnit.HOURS), now.plusSeconds(1));
        JsonNode first = json(post("/v1/deliveries/replay", body));
        assertThat(first.get("replayed").asInt()).isEqualTo(2);
        JsonNode second = json(post("/v1/deliveries/replay", body));
        assertThat(second.get("replayed").asInt()).as("already replayed").isZero();

        List<UUID> replayedFrom = jdbc.sql("SELECT replay_of FROM deliveries WHERE endpoint_id = ? AND replay_of IS NOT NULL ORDER BY created_at")
                .param(endpoint).query(UUID.class).list();
        assertThat(replayedFrom).containsExactlyInAnyOrder(inRange1, inRange2);

        assertThat(post("/v1/deliveries/replay", "{\"endpointId\":\"%s\",\"from\":\"%s\",\"to\":\"%s\"}"
                .formatted(endpoint, now, now.minusSeconds(5)))).hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(post("/v1/deliveries/replay", "{\"endpointId\":\"%s\",\"from\":\"%s\",\"to\":\"%s\"}"
                .formatted(UUID.randomUUID(), now.minusSeconds(5), now))).hasStatus(HttpStatus.NOT_FOUND);
    }
}
