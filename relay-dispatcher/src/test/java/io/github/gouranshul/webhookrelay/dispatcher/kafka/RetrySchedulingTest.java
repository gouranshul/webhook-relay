package io.github.gouranshul.webhookrelay.dispatcher.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gouranshul.webhookrelay.dispatcher.config.DispatcherProperties;
import io.github.gouranshul.webhookrelay.dispatcher.delivery.RetryableDeliveryException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;

class RetrySchedulingTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final DispatcherProperties.Retry RETRY = new DispatcherProperties.Retry(
            6, Duration.ofSeconds(1), 5, Duration.ofMinutes(10), 0.2, Duration.ofHours(1));

    private final RetryScheduling scheduling = new RetryScheduling(
            new DispatcherProperties(null, null, null, RETRY, null, null), Clock.fixed(NOW, ZoneOffset.UTC));

    private static ConsumerRecord<String, String> recordAfterAttempts(Integer attempts) {
        var record = new ConsumerRecord<>("webhook.deliveries", 0, 0L, "k", "v");
        if (attempts != null) {
            record.headers().add(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS,
                    ByteBuffer.allocate(Integer.BYTES).putInt(attempts).array());
        }
        return record;
    }

    private static long dueOffsetMillis(Headers headers) {
        byte[] value = headers.lastHeader(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP).value();
        return new BigInteger(value).longValue() - NOW.toEpochMilli();
    }

    @Test
    void defaultDelaysAreExponentialAndCapped() {
        assertThat(RETRY.delayBeforeRetry(0)).isEqualTo(Duration.ofSeconds(1));
        assertThat(RETRY.delayBeforeRetry(1)).isEqualTo(Duration.ofSeconds(5));
        assertThat(RETRY.delayBeforeRetry(2)).isEqualTo(Duration.ofSeconds(25));
        assertThat(RETRY.delayBeforeRetry(3)).isEqualTo(Duration.ofSeconds(125));
        assertThat(RETRY.delayBeforeRetry(4)).isEqualTo(Duration.ofMinutes(10));
    }

    @RepeatedTest(20)
    void addsUpToTwentyPercentJitterPerMessage() {
        long first = dueOffsetMillis(scheduling.dueTimestampHeader(recordAfterAttempts(null), new RuntimeException()));
        long third = dueOffsetMillis(scheduling.dueTimestampHeader(recordAfterAttempts(3), new RuntimeException()));
        assertThat(first).isBetween(1_000L, 1_200L);
        assertThat(third).isBetween(25_000L, 30_000L);
    }

    @Test
    void jitterActuallyVariesBetweenMessages() {
        var offsets = new java.util.HashSet<Long>();
        for (int i = 0; i < 50; i++) {
            offsets.add(dueOffsetMillis(scheduling.dueTimestampHeader(recordAfterAttempts(2), new RuntimeException())));
        }
        assertThat(offsets).hasSizeGreaterThan(10);
    }

    @Test
    void retryAfterWinsWhenLongerAndIsCapped() {
        var cause = new RuntimeException(new RetryableDeliveryException("429", Duration.ofSeconds(90)));
        assertThat(dueOffsetMillis(scheduling.dueTimestampHeader(recordAfterAttempts(null), cause))).isEqualTo(90_000);

        var shortHint = new RetryableDeliveryException("503", Duration.ofMillis(10));
        assertThat(dueOffsetMillis(scheduling.dueTimestampHeader(recordAfterAttempts(null), shortHint)))
                .isBetween(1_000L, 1_200L);

        var hostile = new RetryableDeliveryException("503", Duration.ofDays(30));
        assertThat(dueOffsetMillis(scheduling.dueTimestampHeader(recordAfterAttempts(null), hostile)))
                .isEqualTo(Duration.ofHours(1).toMillis());
    }

    @Test
    void noScheduleOnceAttemptsAreExhausted() {
        assertThat(scheduling.dueTimestampHeader(recordAfterAttempts(6), new RuntimeException())).isNull();
        assertThat(RetryScheduling.attemptsSoFar(recordAfterAttempts(null))).isEqualTo(1);
        assertThat(RetryScheduling.attemptsSoFar(recordAfterAttempts(4))).isEqualTo(4);
    }
}
