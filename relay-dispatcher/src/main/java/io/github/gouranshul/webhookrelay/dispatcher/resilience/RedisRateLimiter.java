package io.github.gouranshul.webhookrelay.dispatcher.resilience;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * Per-endpoint rate limit, enforced across all dispatcher instances by a Redis token bucket (Lua, atomic).
 * Capacity equals one second of rate, so an endpoint sees at most a one-second burst.
 *
 * <p>Fails open: if Redis is unreachable the call is allowed and {@code webhook.ratelimit.errors} counts it.
 * Rate limiting protects receivers; it should not turn a Redis outage into a delivery outage.
 */
@Component
public class RedisRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RedisRateLimiter.class);
    private static final String KEY_PREFIX = "webhook:ratelimit:";
    private static final long MIN_SLEEP_MS = 5;

    private final StringRedisTemplate redis;
    private final RedisScript<Long> script;
    private final Counter throttled;
    private final Counter errors;
    private final AtomicLong lastErrorLog = new AtomicLong();

    public RedisRateLimiter(StringRedisTemplate redis, MeterRegistry registry) {
        this.redis = redis;
        this.script = RedisScript.of(new ClassPathResource("scripts/token_bucket.lua"), Long.class);
        this.throttled = Counter.builder("webhook.ratelimit.waits").description("Attempts that waited for a token")
                .register(registry);
        this.errors = Counter.builder("webhook.ratelimit.errors").description("Redis errors (failed open)")
                .register(registry);
    }

    /** Takes a token if one is available now; otherwise returns the wait in ms. Never throws. */
    public long tryAcquire(UUID endpointId, int ratePerSecond) {
        try {
            Long wait = redis.execute(script, List.of(KEY_PREFIX + endpointId),
                    Integer.toString(ratePerSecond), Integer.toString(Math.max(1, ratePerSecond)));
            return wait == null ? 0 : wait;
        } catch (RuntimeException e) {
            errors.increment();
            long now = System.currentTimeMillis();
            long last = lastErrorLog.get();
            if (now - last > 10_000 && lastErrorLog.compareAndSet(last, now)) {
                log.warn("Rate limiter unavailable, failing open: {}", e.toString());
            }
            return 0;
        }
    }

    /**
     * Blocks the calling (virtual) thread until a token is taken or {@code maxWait} would be exceeded.
     *
     * @return true if a token was taken
     */
    public boolean acquire(UUID endpointId, int ratePerSecond, Duration maxWait) throws InterruptedException {
        long deadline = System.nanoTime() + maxWait.toNanos();
        boolean waited = false;
        while (true) {
            long waitMs = tryAcquire(endpointId, ratePerSecond);
            if (waitMs == 0) {
                if (waited) {
                    throttled.increment();
                }
                return true;
            }
            long sleep = Math.max(MIN_SLEEP_MS, waitMs);
            if (System.nanoTime() + Duration.ofMillis(sleep).toNanos() > deadline) {
                return false;
            }
            waited = true;
            Thread.sleep(sleep);
        }
    }
}
