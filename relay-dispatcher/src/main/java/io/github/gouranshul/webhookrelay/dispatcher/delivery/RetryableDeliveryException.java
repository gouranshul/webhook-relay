package io.github.gouranshul.webhookrelay.dispatcher.delivery;

import java.time.Duration;

/**
 * The delivery should be attempted again later: 5xx, 408, 429, a timeout, a connection error, or a local
 * deferral (circuit open, bulkhead full, rate limited). Routes the record to the next retry topic.
 */
public class RetryableDeliveryException extends RuntimeException {

    private final transient Duration retryAfter;

    public RetryableDeliveryException(String message) {
        this(message, null);
    }

    public RetryableDeliveryException(String message, Duration retryAfter) {
        super(message, null, false, false);
        this.retryAfter = retryAfter;
    }

    /** The receiver's {@code Retry-After}, if it sent one. */
    public Duration retryAfter() {
        return retryAfter;
    }
}
