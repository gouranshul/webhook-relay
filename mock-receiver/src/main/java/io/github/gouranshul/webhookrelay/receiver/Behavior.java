package io.github.gouranshul.webhookrelay.receiver;

import java.util.List;

/**
 * How a hook responds.
 *
 * @param failureRate       probability in [0,1] of answering with {@code failureStatus}
 * @param minLatencyMs      lower bound of the uniformly random processing delay
 * @param maxLatencyMs      upper bound of the processing delay
 * @param failureStatus     status code used for random failures (e.g. 500, 503, 429)
 * @param retryAfterSeconds if set, sent as {@code Retry-After} on failures
 * @param script            statuses returned for the first N requests, in order, before random behaviour applies
 * @param secret            signing secret; when set, requests with a bad signature get 401
 */
public record Behavior(
        double failureRate,
        long minLatencyMs,
        long maxLatencyMs,
        int failureStatus,
        Integer retryAfterSeconds,
        List<Integer> script,
        String secret) {

    public Behavior {
        if (failureRate < 0 || failureRate > 1) {
            throw new IllegalArgumentException("failureRate must be within [0,1]");
        }
        if (minLatencyMs < 0 || maxLatencyMs < minLatencyMs) {
            throw new IllegalArgumentException("latency bounds are invalid");
        }
        script = script == null ? List.of() : List.copyOf(script);
        failureStatus = failureStatus == 0 ? 503 : failureStatus;
    }

    public static Behavior healthy() {
        return new Behavior(0, 0, 0, 503, null, List.of(), null);
    }

    public Behavior withSecret(String newSecret) {
        return new Behavior(failureRate, minLatencyMs, maxLatencyMs, failureStatus, retryAfterSeconds, script, newSecret);
    }

    @Override
    public String toString() {
        return "Behavior[failureRate=" + failureRate + ", latency=" + minLatencyMs + "-" + maxLatencyMs
                + "ms, failureStatus=" + failureStatus + ", script=" + script + ", secret="
                + (secret == null ? "none" : "<redacted>") + "]";
    }
}
