package io.github.gouranshul.webhookrelay.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gouranshul.webhookrelay.receiver.Behavior;
import io.github.gouranshul.webhookrelay.receiver.HookState;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class E2eTestSupport {

    static final List<String> NON_TERMINAL = List.of("PENDING", "IN_FLIGHT", "RETRYING", "PARKED");

    protected RelayStack stack;
    private final HttpClient http = HttpClient.newHttpClient();

    /** Dispatcher property overrides for this class, e.g. {@code "relay.resilience.bulkhead-per-endpoint=20"}. */
    protected String[] dispatcherOverrides() {
        return new String[0];
    }

    @BeforeAll
    void startStack() throws Exception {
        stack = new RelayStack().start(dispatcherOverrides());
    }

    @AfterAll
    void stopStack() {
        if (stack != null) {
            stack.close();
        }
    }

    record Endpoint(UUID id, String hook, String secret) {
    }

    protected Endpoint registerEndpoint(String type, Behavior behavior) throws Exception {
        String hook = "hook-" + UUID.randomUUID();
        String body = "{\"url\":\"%s\",\"eventTypes\":[\"%s\"],\"rateLimitPerSec\":1000}"
                .formatted(stack.receiver.url(hook), type);
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(stack.apiUrl() + "/v1/endpoints"))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer " + RelayStack.API_TOKEN)
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(201);
        UUID id = UUID.fromString(field(response.body(), "id"));
        String secret = field(response.body(), "secret");
        stack.receiver.state().configure(hook, behavior.withSecret(secret));
        return new Endpoint(id, hook, secret);
    }

    protected void setBehavior(Endpoint endpoint, Behavior behavior) {
        stack.receiver.state().configure(endpoint.hook(), behavior.withSecret(endpoint.secret()));
    }

    protected static Behavior realistic() {
        // 10% failures (503), 20-80 ms latency: a flaky but alive receiver.
        return new Behavior(0.10, 20, 80, 503, null, null, null);
    }

    /** Snapshot of where every delivery of {@code type} ended up. */
    record Audit(long events, long deliveries, Map<String, Long> byStatus, long attempts, long receiverRequests,
            long receiverDuplicates, long succeededButNotReceived) {

        long nonTerminal() {
            return NON_TERMINAL.stream().mapToLong(s -> byStatus.getOrDefault(s, 0L)).sum();
        }

        @Override
        public String toString() {
            return "events=" + events + " deliveries=" + deliveries + " byStatus=" + byStatus + " attempts=" + attempts
                    + " receiverRequests=" + receiverRequests + " receiverDuplicates=" + receiverDuplicates
                    + " succeededButNotReceived=" + succeededButNotReceived;
        }
    }

    protected Audit audit(String type, List<Endpoint> endpoints) {
        long events = stack.jdbc.sql("SELECT count(*) FROM events WHERE event_type = ?").param(type)
                .query(Long.class).single();
        Map<String, Long> byStatus = stack.jdbc.sql("""
                        SELECT d.status, count(*) FROM deliveries d JOIN events e ON e.id = d.event_id
                         WHERE e.event_type = ? GROUP BY d.status""")
                .param(type)
                .query((rs, n) -> Map.entry(rs.getString(1), rs.getLong(2)))
                .list().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        long deliveries = byStatus.values().stream().mapToLong(Long::longValue).sum();
        long attempts = stack.jdbc.sql("""
                        SELECT count(*) FROM delivery_attempts a JOIN deliveries d ON d.id = a.delivery_id
                          JOIN events e ON e.id = d.event_id WHERE e.event_type = ?""")
                .param(type).query(Long.class).single();
        List<String> succeeded = stack.jdbc.sql("""
                        SELECT d.webhook_id::text FROM deliveries d JOIN events e ON e.id = d.event_id
                         WHERE e.event_type = ? AND d.status = 'SUCCEEDED'""")
                .param(type).query(String.class).list();
        List<HookState> hooks = new ArrayList<>();
        endpoints.forEach(e -> hooks.add(stack.receiver.state().hook(e.hook())));
        long notReceived = succeeded.stream().filter(id -> hooks.stream().noneMatch(h -> h.hasSucceeded(id))).count();
        long requests = hooks.stream().mapToLong(h -> h.stats().requests()).sum();
        long duplicates = hooks.stream().mapToLong(h -> h.stats().duplicates()).sum();
        return new Audit(events, deliveries, byStatus, attempts, requests, duplicates, notReceived);
    }

    /** Polls until no delivery of {@code type} is in a non-terminal state. */
    protected Audit awaitDrained(String type, List<Endpoint> endpoints, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        Audit audit = audit(type, endpoints);
        while (audit.nonTerminal() > 0 && System.nanoTime() < deadline) {
            Thread.sleep(1000);
            audit = audit(type, endpoints);
        }
        return audit;
    }

    /**
     * The zero-loss invariant: every event the producer meant to send is stored exactly once, every subscribed
     * endpoint has a delivery for it, every delivery ended in SUCCEEDED or FAILED (DLT), and every SUCCEEDED
     * delivery really reached the receiver.
     */
    protected void assertNothingLost(Traffic traffic, Audit audit, int fanOut) {
        assertThat(traffic.gaveUp.get()).as("producer requests that never got a 202").isZero();
        assertThat(audit.events()).as("events stored (idempotent retries must not duplicate)").isEqualTo(traffic.sent.get());
        assertThat(audit.deliveries()).as("one delivery per event per endpoint").isEqualTo((long) traffic.sent.get() * fanOut);
        assertThat(audit.nonTerminal()).as("deliveries not yet SUCCEEDED or FAILED").isZero();
        assertThat(audit.byStatus().getOrDefault("SUCCEEDED", 0L) + audit.byStatus().getOrDefault("FAILED", 0L))
                .isEqualTo(audit.deliveries());
        assertThat(audit.succeededButNotReceived()).as("SUCCEEDED deliveries the receiver never saw").isZero();
    }

    protected static String uniqueType(String prefix) {
        return prefix + ".e" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }

    private static String field(String json, String name) {
        Matcher m = Pattern.compile("\"" + name + "\":\"([^\"]+)\"").matcher(json);
        if (!m.find()) {
            throw new IllegalStateException(name + " not found in " + json);
        }
        return m.group(1);
    }
}
