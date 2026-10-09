package io.github.gouranshul.webhookrelay.dispatcher.kafka;

import io.github.gouranshul.webhookrelay.dispatcher.config.DispatcherProperties;
import io.github.gouranshul.webhookrelay.dispatcher.delivery.RetryableDeliveryException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.retrytopic.DeadLetterPublishingRecovererFactory;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Two adjustments to Spring Kafka's retry topics.
 *
 * <p><b>Per-message jitter and Retry-After.</b> Each retry topic has one delay, fixed at startup (even with
 * {@code @BackOff(jitter)}, the jitter is drawn once per topic, not per record). Records that fail together
 * would retry together, which is how a receiver recovering from an outage gets knocked over again. When a
 * record is forwarded to a retry topic, this appends a later {@code retry_topic-backoff-timestamp} header:
 * {@code delay * (1 + U[0, jitter))}, or the receiver's {@code Retry-After} if that is longer. The retry
 * consumer reads the last such header, so it pauses that partition until the per-record due time.
 *
 * <p><b>Blocking retries for our own infrastructure.</b> A database error says nothing about the receiver.
 * Retrying it in place (with backoff) instead of hopping retry topics means a short Postgres outage does not
 * burn a delivery's attempts and push it to the DLT.
 */
@Configuration(proxyBeanMethods = false)
public class RetryScheduling extends RetryTopicConfigurationSupport {

    private final DispatcherProperties.Retry retry;
    private final Clock clock;

    public RetryScheduling(DispatcherProperties properties, Clock clock) {
        this.retry = properties.retry();
        this.clock = clock;
    }

    @Override
    protected Consumer<DeadLetterPublishingRecovererFactory> configureDeadLetterPublishingContainerFactory() {
        return factory -> factory.setHeadersFunction(this::dueTimestampHeader);
    }

    @Override
    protected void configureBlockingRetries(BlockingRetriesConfigurer blockingRetries) {
        ExponentialBackOff backOff = new ExponentialBackOff(500, 2.0);
        backOff.setMaxInterval(10_000);
        backOff.setMaxElapsedTime(120_000);
        blockingRetries.retryOn(DataAccessException.class).backOff(backOff);
    }

    Headers dueTimestampHeader(ConsumerRecord<?, ?> record, Exception exception) {
        int failedAttempt = attemptsSoFar(record);
        if (failedAttempt >= retry.attempts()) {
            return null; // on its way to the DLT
        }
        Duration base = retry.delayBeforeRetry(failedAttempt - 1);
        long jitterMillis = (long) (base.toMillis() * retry.jitter() * ThreadLocalRandom.current().nextDouble());
        Duration wait = base.plusMillis(jitterMillis);
        Duration retryAfter = retryAfter(exception);
        if (retryAfter != null) {
            Duration capped = retryAfter.compareTo(retry.maxRetryAfter()) > 0 ? retry.maxRetryAfter() : retryAfter;
            if (capped.compareTo(wait) > 0) {
                wait = capped;
            }
        }
        long due = clock.millis() + wait.toMillis();
        Headers headers = new RecordHeaders();
        headers.add(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP, BigInteger.valueOf(due).toByteArray());
        return headers;
    }

    /** Attempts made so far, including the one that just failed. Absent on the main topic, i.e. 1. */
    static int attemptsSoFar(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS);
        if (header == null || header.value() == null || header.value().length != Integer.BYTES) {
            return 1;
        }
        return ByteBuffer.wrap(header.value()).getInt();
    }

    private static Duration retryAfter(Throwable exception) {
        for (Throwable t = exception; t != null; t = t.getCause()) {
            if (t instanceof RetryableDeliveryException rde) {
                return rde.retryAfter();
            }
        }
        return null;
    }
}
