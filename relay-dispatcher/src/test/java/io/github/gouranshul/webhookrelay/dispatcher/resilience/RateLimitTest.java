package io.github.gouranshul.webhookrelay.dispatcher.resilience;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gouranshul.webhookrelay.dispatcher.DispatcherIntegrationTest;
import io.github.gouranshul.webhookrelay.receiver.Behavior;
import io.github.gouranshul.webhookrelay.receiver.HookState;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(properties = {
    "relay.resilience.bulkhead-per-endpoint=30",
    "relay.resilience.rate-limit-max-wait=10s",
    "relay.dispatcher.request-timeout=2s"
})
class RateLimitTest extends DispatcherIntegrationTest {

    @Autowired
    RedisRateLimiter limiter;

    @Test
    void tokenBucketAllowsBurstThenRefillsAtRate() {
        UUID endpoint = UUID.randomUUID();
        int granted = 0;
        for (int i = 0; i < 10; i++) {
            if (limiter.tryAcquire(endpoint, 5) == 0) {
                granted++;
            }
        }
        assertThat(granted).as("burst capacity = one second of rate").isEqualTo(5);
        long wait = limiter.tryAcquire(endpoint, 5);
        assertThat(wait).isBetween(1L, 200L);
    }

    @Test
    void deliveriesToARateLimitedEndpointAreSpreadOutNotDropped() {
        TestEndpoint limited = endpoint(Behavior.healthy(), 5);
        for (int i = 0; i < 25; i++) {
            enqueue(limited);
        }

        await().atMost(Duration.ofSeconds(30)).until(() -> hook(limited).stats().uniqueSucceeded() == 25);

        List<Instant> times = hook(limited).recent().stream().map(HookState.Received::receivedAt).sorted().toList();
        Duration span = Duration.between(times.getFirst(), times.getLast());
        // 5 immediately (burst), the other 20 at 5/s: at least ~4 s overall.
        assertThat(span).isGreaterThanOrEqualTo(Duration.ofMillis(3600));
        // No one-second window may exceed burst + rate.
        for (Instant start : times) {
            long inWindow = times.stream().filter(t -> !t.isBefore(start) && t.isBefore(start.plusSeconds(1))).count();
            assertThat(inWindow).isLessThanOrEqualTo(10);
        }
        assertThat(hook(limited).stats().duplicates()).isZero();
    }
}
