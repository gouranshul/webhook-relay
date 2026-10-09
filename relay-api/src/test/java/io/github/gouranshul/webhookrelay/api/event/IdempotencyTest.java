package io.github.gouranshul.webhookrelay.api.event;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gouranshul.webhookrelay.api.ApiIntegrationTest;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

class IdempotencyTest extends ApiIntegrationTest {

    private long eventsOfType(String type) {
        return jdbc.sql("SELECT count(*) FROM events WHERE event_type = ?").param(type).query(Long.class).single();
    }

    @Test
    void duplicateKeyReturnsOriginalResponseAndCreatesNothing() {
        String type = uniqueType("order");
        createEndpoint(List.of(type));
        String key = "key-" + UUID.randomUUID();
        String body = "{\"type\":\"%s\",\"data\":{\"order_id\":42}}".formatted(type);

        MvcTestResult first = post("/v1/events", body, "Idempotency-Key", key);
        MvcTestResult second = post("/v1/events", body, "Idempotency-Key", key);

        assertThat(first).hasStatus(HttpStatus.ACCEPTED);
        assertThat(second).hasStatus(HttpStatus.ACCEPTED);
        assertThat(first.getResponse().getHeader("Idempotent-Replayed")).isEqualTo("false");
        assertThat(second.getResponse().getHeader("Idempotent-Replayed")).isEqualTo("true");
        assertThat(json(second)).isEqualTo(json(first));
        assertThat(json(first).get("deliveries").asInt()).isEqualTo(1);
        assertThat(eventsOfType(type)).isEqualTo(1);
        assertThat(jdbc.sql("SELECT count(*) FROM deliveries d JOIN events e ON e.id = d.event_id WHERE e.event_type = ?")
                .param(type).query(Long.class).single()).isEqualTo(1);
    }

    @Test
    void reusingKeyWithDifferentBodyIsRejected() {
        String type = uniqueType("order");
        String key = "key-" + UUID.randomUUID();
        assertThat(post("/v1/events", "{\"type\":\"%s\",\"data\":{\"a\":1}}".formatted(type), "Idempotency-Key", key))
                .hasStatus(HttpStatus.ACCEPTED);
        assertThat(post("/v1/events", "{\"type\":\"%s\",\"data\":{\"a\":2}}".formatted(type), "Idempotency-Key", key))
                .hasStatus(HttpStatus.UNPROCESSABLE_CONTENT);
        assertThat(eventsOfType(type)).isEqualTo(1);
    }

    @Test
    void concurrentDuplicatesCreateExactlyOneEvent() throws Exception {
        String type = uniqueType("user");
        createEndpoint(List.of(type));
        String key = "key-" + UUID.randomUUID();
        String body = "{\"type\":\"%s\",\"data\":{\"user\":\"u1\"}}".formatted(type);
        int threads = 16;
        CountDownLatch start = new CountDownLatch(1);
        Set<String> ids = ConcurrentHashMap.newKeySet();
        List<Future<Integer>> statuses = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < threads; i++) {
                statuses.add(pool.submit(() -> {
                    start.await();
                    MvcTestResult result = post("/v1/events", body, "Idempotency-Key", key);
                    ids.add(json(result).get("id").asString());
                    return result.getResponse().getStatus();
                }));
            }
            start.countDown();
            for (Future<Integer> status : statuses) {
                assertThat(status.get()).isEqualTo(202);
            }
        }
        assertThat(ids).hasSize(1);
        assertThat(eventsOfType(type)).isEqualTo(1);
    }

    @Test
    void expiredKeyCanBeReused() {
        String type = uniqueType("invoice");
        String key = "key-" + UUID.randomUUID();
        String body = "{\"type\":\"%s\",\"data\":{\"invoice\":\"i1\"}}".formatted(type);
        String firstId = json(post("/v1/events", body, "Idempotency-Key", key)).get("id").asString();
        jdbc.sql("UPDATE idempotency_keys SET expires_at = now() - interval '1 second' WHERE idempotency_key = ?")
                .param(key).update();

        MvcTestResult again = post("/v1/events", body, "Idempotency-Key", key);

        assertThat(again.getResponse().getHeader("Idempotent-Replayed")).isEqualTo("false");
        assertThat(json(again).get("id").asString()).isNotEqualTo(firstId);
        assertThat(eventsOfType(type)).isEqualTo(2);
        assertThat(jdbc.sql("SELECT expires_at > now() + interval '23 hours' FROM idempotency_keys WHERE idempotency_key = ?")
                .param(key).query(Boolean.class).single()).isTrue();
    }

    @Test
    void withoutKeyEveryRequestCreatesAnEvent() {
        String type = uniqueType("order");
        String body = "{\"type\":\"%s\",\"data\":{}}".formatted(type);
        post("/v1/events", body);
        post("/v1/events", body);
        assertThat(eventsOfType(type)).isEqualTo(2);
    }

    @Test
    void rejectsInvalidEvents() {
        assertThat(post("/v1/events", "{\"type\":\"order.created\",\"data\":[1,2]}")).hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(post("/v1/events", "{\"type\":\"Order Created\",\"data\":{}}")).hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(post("/v1/events", "{\"type\":\"order.created\"}")).hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(post("/v1/events", "{\"type\":\"order.created\",\"data\":{}}", "Idempotency-Key", " "))
                .hasStatus(HttpStatus.BAD_REQUEST);
        String huge = "{\"type\":\"order.created\",\"data\":{\"blob\":\"" + "x".repeat(300_000) + "\"}}";
        assertThat(post("/v1/events", huge)).hasStatus(HttpStatus.CONTENT_TOO_LARGE);
    }
}
