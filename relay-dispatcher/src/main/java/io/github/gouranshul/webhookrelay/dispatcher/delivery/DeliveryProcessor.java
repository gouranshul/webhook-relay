package io.github.gouranshul.webhookrelay.dispatcher.delivery;

import io.github.gouranshul.webhookrelay.common.model.DeliveryMessage;
import io.github.gouranshul.webhookrelay.common.model.DeliveryStatus;
import io.github.gouranshul.webhookrelay.dispatcher.config.DispatcherProperties;
import io.github.gouranshul.webhookrelay.dispatcher.endpoint.EndpointCache;
import io.github.gouranshul.webhookrelay.dispatcher.endpoint.EndpointConfig;
import io.github.gouranshul.webhookrelay.dispatcher.http.HttpOutcome;
import io.github.gouranshul.webhookrelay.dispatcher.http.WebhookSender;
import io.github.gouranshul.webhookrelay.dispatcher.resilience.EndpointGuards;
import io.github.gouranshul.webhookrelay.dispatcher.resilience.RedisRateLimiter;
import io.github.gouranshul.webhookrelay.dispatcher.tracing.OutboundTracing;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Delivers one message without letting a slow or failing endpoint hold up anyone else.
 *
 * <p>On the consumer thread, only fast, non-blocking steps run: endpoint lookup (cached), circuit breaker,
 * bulkhead and global in-flight permits, and the claim (one indexed UPDATE). The slow part (waiting for a
 * rate-limit token, the HTTP call) runs on the delivery executor. The consumer waits for it at most
 * {@code handoffAfter}; past that the record is passed to the next retry topic and the attempt finishes in
 * the background, recording its own result. The retry copy later finds the delivery SUCCEEDED (and skips it)
 * or RETRYING (and tries again).
 *
 * <p>Returns normally when the record needs no more work. Throws {@link RetryableDeliveryException} to
 * schedule a retry and {@link PermanentDeliveryException} to dead-letter.
 */
@Component
public class DeliveryProcessor {

    private static final Logger log = LoggerFactory.getLogger(DeliveryProcessor.class);

    /** What a background attempt produced. */
    sealed interface AttemptResult permits Sent, Deferred {
    }

    record Sent(HttpOutcome outcome) implements AttemptResult {
    }

    record Deferred(String reason) implements AttemptResult {
    }

    private final EndpointCache endpoints;
    private final DeliveryRepository deliveries;
    private final WebhookSender sender;
    private final DeliveryMetrics metrics;
    private final OutboundTracing tracing;
    private final EndpointGuards guards;
    private final RedisRateLimiter rateLimiter;
    private final ExecutorService executor;
    private final DispatcherProperties.Resilience resilience;
    private final Semaphore inFlight;
    private final Duration lease;
    private final Duration handedOffRetryAfter;

    public DeliveryProcessor(EndpointCache endpoints, DeliveryRepository deliveries, WebhookSender sender,
            DeliveryMetrics metrics, OutboundTracing tracing, EndpointGuards guards, RedisRateLimiter rateLimiter,
            ExecutorService deliveryExecutor, DispatcherProperties properties, MeterRegistry registry) {
        this.endpoints = endpoints;
        this.deliveries = deliveries;
        this.sender = sender;
        this.metrics = metrics;
        this.tracing = tracing;
        this.guards = guards;
        this.rateLimiter = rateLimiter;
        this.executor = deliveryExecutor;
        this.resilience = properties.resilience();
        this.inFlight = new Semaphore(resilience.maxInFlight());
        // The lease must outlive the longest legitimate attempt: token wait + request timeout, plus a margin.
        this.lease = properties.dispatcher().requestTimeout()
                .plus(resilience.rateLimitEnabled() ? resilience.rateLimitMaxWait() : Duration.ZERO)
                .plus(properties.dispatcher().leaseMargin());
        // A handed-off attempt is certainly finished (one way or the other) after token wait + request timeout.
        // Scheduling its retry copy for then avoids burning retry slots on "still in flight".
        Duration attemptBound = properties.dispatcher().requestTimeout()
                .plus(resilience.rateLimitEnabled() ? resilience.rateLimitMaxWait() : Duration.ZERO);
        this.handedOffRetryAfter = attemptBound.minus(resilience.handoffAfter()).plusMillis(250);
        Gauge.builder("webhook.delivery.inflight", inFlight, s -> resilience.maxInFlight() - s.availablePermits())
                .description("HTTP attempts currently running")
                .register(registry);
    }

    public void process(DeliveryMessage message) {
        EndpointConfig endpoint = endpoints.get(message.endpointId());
        if (!isDeliverable(message, endpoint)) {
            return;
        }

        EndpointGuards.Guard guard = guards.forEndpoint(message.endpointId());
        CircuitBreaker breaker = guard.circuitBreaker();
        Bulkhead bulkhead = guard.bulkhead();

        if (!breaker.tryAcquirePermission()) {
            defer("circuit_open", "circuit breaker open; endpoint not called");
        }
        if (!bulkhead.tryAcquirePermission()) {
            breaker.releasePermission();
            defer("bulkhead_full", "endpoint already has " + resilience.bulkheadPerEndpoint() + " attempts in flight");
        }
        if (!inFlight.tryAcquire()) {
            bulkhead.onComplete();
            breaker.releasePermission();
            defer("dispatcher_saturated", "dispatcher at its in-flight limit");
        }

        // From here on the permits are released exactly once: by the background attempt, or below if the
        // attempt never starts.
        OptionalInt claimed;
        try {
            claimed = deliveries.claim(message.deliveryId(), lease);
        } catch (RuntimeException e) {
            releaseAll(breaker, bulkhead);
            throw e;
        }
        if (claimed.isEmpty()) {
            releaseAll(breaker, bulkhead);
            handleUnclaimable(message);
            return;
        }
        int attempt = claimed.getAsInt();
        Map<String, String> traceHeaders = tracing.currentHeaders();

        Future<AttemptResult> future;
        try {
            future = executor.submit(() -> runAttempt(message, endpoint, attempt, breaker, bulkhead, traceHeaders));
        } catch (RejectedExecutionException e) {
            releaseAll(breaker, bulkhead);
            deliveries.release(message.deliveryId(), "executor rejected the attempt");
            throw new RetryableDeliveryException("executor rejected the attempt");
        }

        AttemptResult result;
        try {
            result = future.get(resilience.handoffAfter().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException slow) {
            metrics.skipped("handed_off");
            throw new RetryableDeliveryException("attempt still running after " + resilience.handoffAfter().toMillis()
                    + " ms; continuing in the background", handedOffRetryAfter);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableDeliveryException("interrupted while waiting for the attempt");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException(e.getCause());
        }

        switch (result) {
            case Deferred deferred -> throw new RetryableDeliveryException(deferred.reason());
            case Sent sent -> {
                HttpOutcome outcome = sent.outcome();
                switch (outcome.kind()) {
                    case SUCCESS -> {
                    }
                    case PERMANENT -> throw new PermanentDeliveryException(outcome.error());
                    default -> throw new RetryableDeliveryException(outcome.error(), outcome.retryAfter());
                }
            }
        }
    }

    /** Runs on the delivery executor. Owns the permits and the delivery's lease until it returns. */
    private AttemptResult runAttempt(DeliveryMessage message, EndpointConfig endpoint, int attempt,
            CircuitBreaker breaker, Bulkhead bulkhead, Map<String, String> traceHeaders) throws InterruptedException {
        boolean called = false;
        try {
            if (resilience.rateLimitEnabled()
                    && !rateLimiter.acquire(endpoint.id(), endpoint.rateLimitPerSec(), resilience.rateLimitMaxWait())) {
                deliveries.release(message.deliveryId(), "rate limited");
                metrics.skipped("rate_limited");
                return new Deferred("rate limit of " + endpoint.rateLimitPerSec() + "/s reached");
            }
            byte[] body = message.toWireBody().getBytes(StandardCharsets.UTF_8);
            HttpOutcome outcome = sender.send(endpoint, message.webhookId().toString(), body, traceHeaders);
            called = true;
            recordInBreaker(breaker, outcome);
            deliveries.recordAttempt(message.deliveryId(), message.endpointId(), attempt, outcome);
            metrics.attempt(attempt, outcome);
            return new Sent(outcome);
        } catch (RuntimeException | InterruptedException e) {
            // Nobody may be waiting for this future (handed off), so log here; the lease expiring lets a retry copy
            // reclaim the delivery.
            log.warn("Attempt {} for delivery {} failed {} the HTTP call: {}", attempt, message.deliveryId(),
                    called ? "after" : "before", e.toString());
            throw e;
        } finally {
            if (!called) {
                breaker.releasePermission();
            }
            bulkhead.onComplete();
            inFlight.release();
        }
    }

    /**
     * Receiver health, not request validity, drives the breaker: 2xx and 4xx both mean the receiver is up
     * and answering; 5xx, 408, 429, timeouts and connection errors count against it.
     */
    private static void recordInBreaker(CircuitBreaker breaker, HttpOutcome outcome) {
        long nanos = outcome.latency().toNanos();
        if (outcome.kind() == HttpOutcome.Kind.SUCCESS || outcome.kind() == HttpOutcome.Kind.PERMANENT) {
            breaker.onSuccess(nanos, TimeUnit.NANOSECONDS);
        } else {
            breaker.onError(nanos, TimeUnit.NANOSECONDS, new RetryableDeliveryException(outcome.kind().name()));
        }
    }

    private void releaseAll(CircuitBreaker breaker, Bulkhead bulkhead) {
        breaker.releasePermission();
        bulkhead.onComplete();
        inFlight.release();
    }

    private void defer(String metric, String reason) {
        metrics.skipped(metric);
        throw new RetryableDeliveryException(reason);
    }

    /** Endpoint deleted: cancel. Paused: park (only if it really is paused right now). */
    private boolean isDeliverable(DeliveryMessage message, EndpointConfig endpoint) {
        switch (endpoint.status()) {
            case DELETED -> {
                deliveries.cancel(message.deliveryId());
                metrics.skipped("endpoint_deleted");
                return false;
            }
            case PAUSED -> {
                if (deliveries.parkIfEndpointPaused(message.deliveryId(), message.endpointId())) {
                    metrics.skipped("endpoint_paused");
                    return false;
                }
                // The cached status was stale; the endpoint has been resumed.
                endpoints.invalidate(message.endpointId());
                return true;
            }
            default -> {
                return true;
            }
        }
    }

    private void handleUnclaimable(DeliveryMessage message) {
        Optional<DeliveryRepository.State> state = deliveries.state(message.deliveryId());
        if (state.isEmpty()) {
            log.warn("Delivery {} not found; dropping record", message.deliveryId());
            metrics.skipped("unknown_delivery");
            return;
        }
        DeliveryStatus status = state.get().status();
        if (status.isTerminal() || status == DeliveryStatus.PARKED) {
            // Redelivered record (outbox republish, rebalance, retry copy of a finished delivery): no second send.
            metrics.skipped("duplicate");
            return;
        }
        // IN_FLIGHT under a live lease: a handed-off attempt or another worker. Check again on the next retry.
        metrics.skipped("in_flight_elsewhere");
        // Come back when the lease runs out, not on the next short retry delay.
        throw new RetryableDeliveryException("delivery is in flight on another worker",
                state.get().leaseRemaining().plusMillis(250));
    }
}
