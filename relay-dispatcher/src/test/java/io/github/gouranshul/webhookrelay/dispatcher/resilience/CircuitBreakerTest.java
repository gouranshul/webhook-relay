package io.github.gouranshul.webhookrelay.dispatcher.resilience;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gouranshul.webhookrelay.common.model.DeliveryMessage;
import io.github.gouranshul.webhookrelay.dispatcher.DispatcherIntegrationTest;
import io.github.gouranshul.webhookrelay.receiver.Behavior;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(properties = {
    "relay.resilience.circuit-breaker.sliding-window-size=4",
    "relay.resilience.circuit-breaker.minimum-calls=4",
    "relay.resilience.circuit-breaker.open-duration=60s"
})
class CircuitBreakerTest extends DispatcherIntegrationTest {

    @Autowired
    EndpointGuards guards;
    @Autowired
    MeterRegistry registry;

    @Test
    void openCircuitStopsCallingTheEndpointAndRoutesToRetryTopics() {
        TestEndpoint down = endpoint(new Behavior(1.0, 0, 0, 503, null, null, null));
        List<DeliveryMessage> messages = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            messages.add(enqueue(down));
        }

        // Every delivery still ends in a terminal state: here, the DLT.
        await().atMost(Duration.ofSeconds(60))
                .until(() -> messages.stream().allMatch(m -> deadLettered(m.deliveryId())));

        assertThat(guards.forEndpoint(down.id()).circuitBreaker().getState()).isEqualTo(CircuitBreaker.State.OPEN);
        // 12 deliveries x 4 attempts would be 48 calls without a breaker. It opens after 4 failures; calls
        // already admitted before it opened may still land.
        long calls = hook(down).stats().requests();
        assertThat(calls).isBetween(4L, 8L);
        assertThat(registry.find("webhook.delivery.skipped").tag("reason", "circuit_open").counter().count())
                .isGreaterThan(30);
        assertThat(registry.find("webhook.circuitbreaker.state").tag("state", "open").gauge().value())
                .isGreaterThanOrEqualTo(1);
        // Deferred tries are not HTTP attempts and are not recorded as such.
        long recordedAttempts = messages.stream().mapToLong(m -> attempts(m.deliveryId()).size()).sum();
        assertThat(recordedAttempts).isEqualTo(calls);
    }

    @Test
    void healthyEndpointKeepsItsBreakerClosed() {
        TestEndpoint healthy = endpoint(Behavior.healthy());
        List<DeliveryMessage> messages = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            messages.add(enqueue(healthy));
        }
        await().atMost(Duration.ofSeconds(20)).until(() -> hook(healthy).stats().uniqueSucceeded() == 6);
        assertThat(guards.forEndpoint(healthy.id()).circuitBreaker().getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }
}
