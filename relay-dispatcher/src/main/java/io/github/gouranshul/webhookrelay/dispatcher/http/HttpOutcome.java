package io.github.gouranshul.webhookrelay.dispatcher.http;

import java.time.Duration;

/**
 * Result of one webhook HTTP attempt, already classified.
 *
 * @param kind       how the relay should react
 * @param statusCode HTTP status, or null when no response was received
 * @param latency    wall time of the attempt
 * @param retryAfter parsed {@code Retry-After}, or null
 * @param error      short, truncated description for {@code delivery_attempts.error}; null on success
 */
public record HttpOutcome(Kind kind, Integer statusCode, Duration latency, Duration retryAfter, String error) {

    public enum Kind {
        /** 2xx. */
        SUCCESS,
        /** 5xx, 408 or 429: try again later. */
        RETRYABLE,
        /** No response within the request timeout: try again later. */
        TIMEOUT,
        /** Connection refused, reset, DNS failure: try again later. */
        CONNECTION_ERROR,
        /** Any other status (4xx, 3xx), or a URL the SSRF policy rejects: do not retry. */
        PERMANENT;

        public boolean retryable() {
            return this == RETRYABLE || this == TIMEOUT || this == CONNECTION_ERROR;
        }
    }

    /** Status classification used for every response. */
    public static Kind classify(int status) {
        if (status >= 200 && status < 300) {
            return Kind.SUCCESS;
        }
        if (status >= 500 || status == 408 || status == 429) {
            return Kind.RETRYABLE;
        }
        return Kind.PERMANENT;
    }
}
