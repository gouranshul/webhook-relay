package io.github.gouranshul.webhookrelay.dispatcher.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("relay")
public record DispatcherProperties(
        @DefaultValue Crypto crypto,
        @DefaultValue Endpoints endpoints,
        @DefaultValue Dispatcher dispatcher,
        @DefaultValue Retry retry,
        @DefaultValue Resilience resilience,
        @DefaultValue AutoPause autoPause,
        @DefaultValue Kafka kafka) {

    /** Pause an endpoint after this many consecutive deliveries reach the DLT. 0 disables. */
    public record AutoPause(@DefaultValue("10") int consecutiveDlt) {
    }

    /**
     * Per-endpoint isolation. See docs/adr/0004-per-endpoint-isolation.md.
     *
     * @param maxInFlight     global cap on concurrent attempts across all endpoints (bounds memory)
     * @param maxEndpoints    cap on per-endpoint guards kept in memory (circuit breaker + bulkhead)
     * @param bulkheadPerEndpoint concurrent attempts allowed per endpoint
     * @param rateLimitMaxWait how long a background attempt may wait for a rate-limit token before deferring
     */
    public record Resilience(
            @DefaultValue("2000") int maxInFlight,
            @DefaultValue("10000") int maxEndpoints,
            @DefaultValue("1h") Duration guardIdleExpiry,
            @DefaultValue("50") int bulkheadPerEndpoint,
            @DefaultValue("true") boolean rateLimitEnabled,
            @DefaultValue("5s") Duration rateLimitMaxWait,
            @DefaultValue CircuitBreaker circuitBreaker) {
    }

    public record CircuitBreaker(
            @DefaultValue("50") float failureRateThreshold,
            @DefaultValue("80") float slowCallRateThreshold,
            @DefaultValue("5s") Duration slowCallDuration,
            @DefaultValue("20") int slidingWindowSize,
            @DefaultValue("10") int minimumCalls,
            @DefaultValue("30s") Duration openDuration,
            @DefaultValue("3") int halfOpenCalls) {
    }

    public record Crypto(String key, @DefaultValue("k1") String keyId) {
    }

    public record Endpoints(
            @DefaultValue("false") boolean allowPrivateNetworks,
            /* Endpoint config (URL, secret, limits, status) is cached; changes apply within this TTL. */
            @DefaultValue("5s") Duration cacheTtl,
            @DefaultValue("10000") int cacheMaxSize) {
    }

    public record Dispatcher(
            /* Consumers for the main topic. Each processes its partitions one record at a time, so parallelism comes from
               partitions x consumers (and instances). They run on virtual threads, so many are cheap. */
            @DefaultValue("16") int concurrency,
            /* Consumers per retry topic and for the DLT; retry traffic is a small fraction of the main topic. */
            @DefaultValue("2") int retryConcurrency,
            @DefaultValue("2s") Duration connectTimeout,
            /* Whole-response timeout for one webhook request. */
            @DefaultValue("10s") Duration requestTimeout,
            /* Extra lease on an in-flight delivery beyond the request timeout, before another worker may reclaim it. */
            @DefaultValue("30s") Duration leaseMargin,
            /* How much of a failed response body is kept in delivery_attempts.error. */
            @DefaultValue("512") int errorBodyLimit,
            /* "virtual" (one virtual thread per HTTP attempt) or "platform" (fixed pool, for comparison). */
            @DefaultValue("virtual") String executor,
            @DefaultValue("200") int platformPoolSize) {
    }

    /**
     * Non-blocking retries. {@code attempts} counts the first try; the defaults give delays of roughly
     * 1s, 5s, 25s, 2m and 10m (capped) before the DLT, each stretched by up to {@code jitter} per message.
     */
    public record Retry(
            @DefaultValue("6") int attempts,
            @DefaultValue("1s") Duration initialDelay,
            @DefaultValue("5") double multiplier,
            @DefaultValue("10m") Duration maxDelay,
            /* Each retry waits delay * (1 + U[0, jitter)). Spreads out retries that failed together. */
            @DefaultValue("0.2") double jitter,
            /* Upper bound on a receiver's Retry-After, so a hostile value cannot park a message for days. */
            @DefaultValue("1h") Duration maxRetryAfter) {

        /** Delay before retry number {@code retryIndex} (0-based): initial * multiplier^i, capped. */
        public Duration delayBeforeRetry(int retryIndex) {
            double millis = initialDelay.toMillis() * Math.pow(multiplier, retryIndex);
            return Duration.ofMillis((long) Math.min(millis, maxDelay.toMillis()));
        }
    }

    public record Kafka(
            @DefaultValue("48") int partitions,
            @DefaultValue("12") int retryPartitions,
            @DefaultValue("1") short replicationFactor) {
    }
}
