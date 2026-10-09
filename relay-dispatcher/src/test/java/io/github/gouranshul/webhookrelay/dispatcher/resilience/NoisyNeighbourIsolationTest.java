package io.github.gouranshul.webhookrelay.dispatcher.resilience;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gouranshul.webhookrelay.common.Topics;
import io.github.gouranshul.webhookrelay.common.model.DeliveryMessage;
import io.github.gouranshul.webhookrelay.dispatcher.DispatcherIntegrationTest;
import io.github.gouranshul.webhookrelay.receiver.Behavior;
import io.github.gouranshul.webhookrelay.receiver.HookState;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

/**
 * Endpoint A answers after 10 s; endpoint B is healthy. Both are keyed onto the SAME partition and a single
 * consumer thread reads it, the worst case for head-of-line blocking. Without isolation every A record would
 * hold the partition for 10 s and B's latency would be measured in minutes. B's p95 must stay under 1 s.
 */
@TestPropertySource(properties = {
    "relay.dispatcher.concurrency=1",
    "relay.dispatcher.request-timeout=15s",
    "relay.resilience.handoff-after=250ms",
    "relay.resilience.bulkhead-per-endpoint=4",
    "relay.retry.attempts=6"
})
class NoisyNeighbourIsolationTest extends DispatcherIntegrationTest {

    private static final Duration B_P95_THRESHOLD = Duration.ofMillis(1000);
    private static final int B_MESSAGES = 200;

    @Autowired
    MeterRegistry registry;

    @Test
    void slowEndpointDoesNotDelayHealthyEndpointOnTheSamePartition() throws Exception {
        int partitions = kafka.partitionsFor(Topics.DELIVERIES).size();
        UUID aId = UUID.randomUUID();
        UUID bId = samePartitionAs(aId, partitions);
        TestEndpoint slow = endpoint(aId, new Behavior(0, 10_000, 10_000, 503, null, null, null), 1000);
        TestEndpoint healthy = endpoint(bId, Behavior.healthy(), 1000);

        Map<String, Instant> publishedAt = new HashMap<>();
        for (int i = 0; i < B_MESSAGES; i++) {
            if (i % 10 == 0) {
                publish(insertDelivery(slow, "order.created", "{\"i\":" + i + "}"), null);
            }
            DeliveryMessage b = insertDelivery(healthy, "order.created", "{\"i\":" + i + "}");
            publishedAt.put(b.webhookId().toString(), Instant.now());
            publish(b, null);
            Thread.sleep(15);
        }

        await().atMost(Duration.ofSeconds(60)).until(() -> hook(healthy).stats().uniqueSucceeded() == B_MESSAGES);

        List<Long> latencies = new ArrayList<>();
        for (HookState.Received r : hook(healthy).recent()) {
            latencies.add(Duration.between(publishedAt.get(r.webhookId()), r.receivedAt()).toMillis());
        }
        latencies.sort(Long::compare);
        long p50 = latencies.get(latencies.size() / 2);
        long p95 = latencies.get((int) Math.ceil(latencies.size() * 0.95) - 1);
        long max = latencies.getLast();
        System.out.printf("Isolation: B p50=%d ms, p95=%d ms, max=%d ms over %d deliveries; A requests so far=%d%n",
                p50, p95, max, latencies.size(), hook(slow).stats().requests());

        assertThat(Duration.ofMillis(p95)).isLessThan(B_P95_THRESHOLD);
        assertThat(hook(healthy).stats().duplicates()).isZero();
        // A's concurrency is capped by its bulkhead; the rest were deferred instead of blocking the partition.
        assertThat(hook(slow).stats().requests()).isLessThanOrEqualTo(8);
        assertThat(registry.find("webhook.delivery.skipped").tag("reason", "bulkhead_full").counter())
                .isNotNull()
                .satisfies(c -> assertThat(c.count()).isPositive());
    }

    /** Finds an endpoint id that the default partitioner puts on the same partition as {@code other}. */
    private static UUID samePartitionAs(UUID other, int partitions) {
        int target = partition(other, partitions);
        while (true) {
            UUID candidate = UUID.randomUUID();
            if (partition(candidate, partitions) == target) {
                return candidate;
            }
        }
    }

    private static int partition(UUID key, int partitions) {
        return Utils.toPositive(Utils.murmur2(key.toString().getBytes(StandardCharsets.UTF_8))) % partitions;
    }
}
