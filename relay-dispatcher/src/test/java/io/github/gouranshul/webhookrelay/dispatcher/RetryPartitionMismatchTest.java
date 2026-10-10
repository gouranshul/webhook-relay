package io.github.gouranshul.webhookrelay.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gouranshul.webhookrelay.common.Topics;
import io.github.gouranshul.webhookrelay.common.model.DeliveryMessage;
import io.github.gouranshul.webhookrelay.common.model.DeliveryStatus;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

/**
 * Regression test for a bug found by the benchmarks. The main topic had more partitions (48) than the retry topics
 * (12), and the recoverer forwarded each failed record to the same partition NUMBER in the retry topic. Records
 * from partitions beyond 11 could not be forwarded at all; they only recovered when the lease sweeper re-enqueued
 * them, over and over, so a 1 s retry took hours. Here the main topic has 6 partitions and the retry topics 3.
 */
@TestPropertySource(properties = "relay.kafka.partitions=6")
class RetryPartitionMismatchTest extends DispatcherIntegrationTest {

    @Autowired
    MeterRegistry registry;

    @Test
    void retriesFromMainPartitionsBeyondTheRetryTopicPartitionCount() {
        int mainPartitions = kafka.partitionsFor(Topics.DELIVERIES).size();
        int retryPartitions = kafka.partitionsFor(Topics.DELIVERIES + "-retry-0").size();
        assertThat(mainPartitions).isGreaterThan(retryPartitions);

        UUID id = UUID.randomUUID();
        while (partition(id, mainPartitions) < retryPartitions) {
            id = UUID.randomUUID();
        }
        TestEndpoint endpoint = endpoint(id, scripted(503, 200), 1000);
        double sweptBefore = registry.get("webhook.delivery.lease_expired").counter().count();

        DeliveryMessage message = enqueue(endpoint);
        awaitStatus(message.deliveryId(), DeliveryStatus.SUCCEEDED, Duration.ofSeconds(30));

        List<Attempt> attempts = attempts(message.deliveryId());
        assertThat(attempts).extracting(Attempt::statusCode).containsExactly(503, 200);
        // Forwarded normally: the retry fired after its ~300 ms delay, not after a lease expiry and a sweep.
        assertThat(Duration.between(attempts.get(0).at(), attempts.get(1).at())).isLessThan(Duration.ofSeconds(5));
        assertThat(registry.get("webhook.delivery.lease_expired").counter().count()).isEqualTo(sweptBefore);
    }

    private static int partition(UUID key, int partitions) {
        return Utils.toPositive(Utils.murmur2(key.toString().getBytes(StandardCharsets.UTF_8))) % partitions;
    }
}
