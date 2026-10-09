package io.github.gouranshul.webhookrelay.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.github.gouranshul.webhookrelay.receiver.Behavior;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Failure injection while traffic is flowing. In every scenario the assertion is the same: ZERO lost events.
 * Every event the producer sent is stored once, and every delivery ends SUCCEEDED or FAILED (on the DLT).
 * Receiver duplicates are reported, not asserted: they are legitimate under at-least-once (ADR 0002).
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ChaosTest extends E2eTestSupport {

    private static final int RATE = 20;
    private static final Duration LOAD = Duration.ofSeconds(30);
    private static final int FAN_OUT = 3;

    private List<Endpoint> endpoints(String type) throws Exception {
        List<Endpoint> endpoints = new ArrayList<>();
        for (int i = 0; i < FAN_OUT; i++) {
            endpoints.add(registerEndpoint(type, realistic()));
        }
        return endpoints;
    }

    private void report(String scenario, Traffic traffic, Audit audit, BacklogSampler backlog, Instant started) {
        System.out.printf("CHAOS %s: sent=%d accepted=%d clientRetries=%d slowestApiResponse=%dms %s | %s | drained in %ds%n",
                scenario, traffic.sent.get(), traffic.accepted.get(), traffic.clientRetries.get(),
                traffic.slowestResponseMs.get(), backlog, audit, Duration.between(started, Instant.now()).toSeconds());
    }

    @Test
    @Order(1)
    void kafkaBrokerRestartMidLoad() throws Exception {
        String type = uniqueType("order");
        List<Endpoint> endpoints = endpoints(type);
        Instant started = Instant.now();
        try (Traffic traffic = new Traffic(stack.apiUrl(), RelayStack.API_TOKEN);
                BacklogSampler backlog = new BacklogSampler(stack.jdbc)) {
            CompletableFuture<Void> chaos = CompletableFuture.runAsync(() -> {
                sleep(Duration.ofSeconds(8));
                stack.restartKafka();
            });
            traffic.run(type, RATE, LOAD);
            chaos.join();
            traffic.awaitResponses(Duration.ofMinutes(3));

            Audit audit = awaitDrained(type, endpoints, Duration.ofMinutes(5));
            report("kafka-restart", traffic, audit, backlog, started);
            assertNothingLost(traffic, audit, FAN_OUT);
        }
    }

    @Test
    @Order(2)
    void fiveSecondsOfPostgresLatency() throws Exception {
        String type = uniqueType("user");
        List<Endpoint> endpoints = endpoints(type);
        Instant started = Instant.now();
        try (Traffic traffic = new Traffic(stack.apiUrl(), RelayStack.API_TOKEN);
                BacklogSampler backlog = new BacklogSampler(stack.jdbc)) {
            CompletableFuture<Void> chaos = CompletableFuture.runAsync(() -> {
                sleep(Duration.ofSeconds(8));
                try {
                    stack.postgresProxy.toxics().latency("pg-latency", ToxicDirection.DOWNSTREAM, 5_000);
                    sleep(Duration.ofSeconds(20));
                    stack.postgresProxy.toxics().get("pg-latency").remove();
                } catch (java.io.IOException e) {
                    throw new IllegalStateException(e);
                }
            });
            traffic.run(type, RATE, LOAD);
            chaos.join();
            traffic.awaitResponses(Duration.ofMinutes(4));

            Audit audit = awaitDrained(type, endpoints, Duration.ofMinutes(5));
            report("postgres-5s-latency", traffic, audit, backlog, started);
            assertNothingLost(traffic, audit, FAN_OUT);
        }
    }

    @Test
    @Order(3)
    void receiverReturns503ForSixtySeconds() throws Exception {
        String type = uniqueType("invoice");
        List<Endpoint> endpoints = endpoints(type);
        Behavior down = new Behavior(1.0, 0, 0, 503, null, null, null);
        Instant started = Instant.now();
        try (Traffic traffic = new Traffic(stack.apiUrl(), RelayStack.API_TOKEN);
                BacklogSampler backlog = new BacklogSampler(stack.jdbc)) {
            CompletableFuture<Void> chaos = CompletableFuture.runAsync(() -> {
                sleep(Duration.ofSeconds(5));
                endpoints.forEach(e -> setBehavior(e, down));
                sleep(Duration.ofSeconds(60));
                endpoints.forEach(e -> setBehavior(e, realistic()));
            });
            traffic.run(type, RATE, LOAD);
            traffic.awaitResponses(Duration.ofMinutes(2));
            chaos.join();

            Audit audit = awaitDrained(type, endpoints, Duration.ofMinutes(6));
            report("receiver-503-60s", traffic, audit, backlog, started);
            assertNothingLost(traffic, audit, FAN_OUT);
            // The retry budget (~5 min) outlasts a 60 s outage, so nearly everything should recover.
            assertThat(audit.byStatus().getOrDefault("SUCCEEDED", 0L)).isGreaterThan(audit.deliveries() * 9 / 10);
        }
    }

    /**
     * The broker is killed and stays down for longer than a forward can wait (delivery.timeout.ms = 30 s) while the receiver is slow and
     * failing, so many attempts are mid-flight when the broker stops. Failed attempts cannot publish their retry record, so their deliveries stay IN_FLIGHT until the lease expires and the
     * sweeper re-enqueues them through the outbox: the path that covers a dispatcher crash.
     */
    @Test
    @Order(4)
    void kafkaOutageWhileAttemptsAreFailing() throws Exception {
        String type = uniqueType("order");
        List<Endpoint> endpoints = new ArrayList<>();
        // 8 s responses against a 5 s request timeout: attempts started just before the crash time out after it.
        Behavior slowAndFailing = new Behavior(1.0, 8000, 8000, 503, null, null, null);
        for (int i = 0; i < FAN_OUT; i++) {
            endpoints.add(registerEndpoint(type, realistic()));
        }
        double requeuedBefore = stack.dispatcherCounter("webhook.delivery.lease_expired");
        Instant started = Instant.now();
        try (Traffic traffic = new Traffic(stack.apiUrl(), RelayStack.API_TOKEN);
                BacklogSampler backlog = new BacklogSampler(stack.jdbc)) {
            CompletableFuture<Void> chaos = CompletableFuture.runAsync(() -> {
                sleep(Duration.ofSeconds(6));
                endpoints.forEach(e -> setBehavior(e, slowAndFailing));
                sleep(Duration.ofSeconds(3));
                try {
                    stack.killKafkaFor(Duration.ofSeconds(45));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                endpoints.forEach(e -> setBehavior(e, realistic()));
            });
            traffic.run(type, RATE, LOAD);
            chaos.join();
            traffic.awaitResponses(Duration.ofMinutes(3));

            Audit audit = awaitDrained(type, endpoints, Duration.ofMinutes(6));
            double requeued = stack.dispatcherCounter("webhook.delivery.lease_expired") - requeuedBefore;
            report("kafka-killed-45s+failing-receiver (lease-sweeper re-enqueued " + (long) requeued + ")",
                    traffic, audit, backlog, started);
            assertNothingLost(traffic, audit, FAN_OUT);
            assertThat(requeued).as("the lease sweeper recovered deliveries whose forward failed").isPositive();
        }
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
