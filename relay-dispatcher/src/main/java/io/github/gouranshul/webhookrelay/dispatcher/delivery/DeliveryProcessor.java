package io.github.gouranshul.webhookrelay.dispatcher.delivery;

import io.github.gouranshul.webhookrelay.common.model.DeliveryMessage;
import io.github.gouranshul.webhookrelay.common.model.DeliveryStatus;
import io.github.gouranshul.webhookrelay.dispatcher.config.DispatcherProperties;
import io.github.gouranshul.webhookrelay.dispatcher.endpoint.EndpointCache;
import io.github.gouranshul.webhookrelay.dispatcher.endpoint.EndpointConfig;
import io.github.gouranshul.webhookrelay.dispatcher.http.HttpOutcome;
import io.github.gouranshul.webhookrelay.dispatcher.http.WebhookSender;
import io.github.gouranshul.webhookrelay.dispatcher.kafka.RetryForwarder;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Delivers one message without letting a slow or failing endpoint hold up anyone else.
 *
 * <p>The consumer thread runs only fast steps: endpoint lookup (cached), circuit breaker and bulkhead permits, a
 * global in-flight permit, and the claim (one indexed UPDATE that also takes a lease). It then hands the
 * attempt to a virtual thread and moves on to the next record. Many attempts for the same endpoint can run at
 * once (up to its bulkhead), so a hot endpoint is not limited to one request per round trip.
 *
 * <p>The attempt finishes the job itself: success is recorded; a retryable failure is forwarded to the next
 * retry topic (and a permanent one to the DLT) through {@link RetryForwarder}, exactly as the framework would.
 * Because the Kafka record was already committed, the delivery row is the durable state: it stays IN_FLIGHT
 * under a lease until the outcome is recorded, and {@link LeaseSweeper} re-enqueues it if that never happens.
 *
 * <p>Decisions made on the consumer thread (circuit open, bulkhead full, already in flight elsewhere) throw
 * {@link RetryableDeliveryException} and the container's {@code @RetryableTopic} error handling forwards them.
 */
@Component
public class DeliveryProcessor {

    private static final Logger log = LoggerFactory.getLogger(DeliveryProcessor.class);
    private static final Duration IN_FLIGHT_WAIT = Duration.ofSeconds(30);

    private final EndpointCache endpoints;
    private final DeliveryRepository deliveries;
    private final WebhookSender sender;
    private final DeliveryMetrics metrics;
    private final OutboundTracing tracing;
    private final EndpointGuards guards;
    private final RedisRateLimiter rateLimiter;
    private final RetryForwarder forwarder;
    private final ExecutorService executor;
    private final DispatcherProperties.Resilience resilience;
    private final Semaphore inFlight;
    private final Duration lease;

    public DeliveryProcessor(EndpointCache endpoints, DeliveryRepository deliveries, WebhookSender sender,
            DeliveryMetrics metrics, OutboundTracing tracing, EndpointGuards guards, RedisRateLimiter rateLimiter,
            RetryForwarder forwarder, ExecutorService deliveryExecutor, DispatcherProperties properties,
            MeterRegistry registry) {
        this.endpoints = endpoints;
        this.deliveries = deliveries;
        this.sender = sender;
        this.metrics = metrics;
        this.tracing = tracing;
        this.guards = guards;
        this.rateLimiter = rateLimiter;
        this.forwarder = forwarder;
        this.executor = deliveryExecutor;
        this.resilience = properties.resilience();
        this.inFlight = new Semaphore(resilience.maxInFlight());
        // The lease must outlive the longest legitimate attempt: token wait + request timeout, plus a margin.
        this.lease = properties.dispatcher().requestTimeout()
                .plus(resilience.rateLimitEnabled() ? resilience.rateLimitMaxWait() : Duration.ZERO)
                .plus(properties.dispatcher().leaseMargin());
        Gauge.builder("webhook.delivery.inflight", inFlight, s -> resilience.maxInFlight() - s.availablePermits())
                .description("HTTP attempts currently running")
                .register(registry);
    }

    /** Called on the consumer thread. Returns once the attempt has been started (or found unnecessary). */
    public void process(ConsumerRecord<String, String> record, DeliveryMessage message) {
        EndpointConfig endpoint = endpoints.get(message.endpointId());
        if (!isDeliverable(message, endpoint)) {
            return;
        }

        EndpointGuards.Guard guard = guards.forEndpoint(message.endpointId());
        CircuitBreaker breaker = guard.circuitBreaker();
        Bulkhead bulkhead = guard.bulkhead();

        if (!breaker.tryAcquirePermission()) {
            throw defer("circuit_open", "circuit breaker open; endpoint not called");
        }
        if (!bulkhead.tryAcquirePermission()) {
            breaker.releasePermission();
            throw defer("bulkhead_full", "endpoint already has " + resilience.bulkheadPerEndpoint() + " attempts in flight");
        }
        // The only place the consumer waits: when the whole dispatcher is saturated. That is back-pressure:
        // consumer lag grows and KEDA adds instances.
        if (!acquireInFlight()) {
            bulkhead.onComplete();
            breaker.releasePermission();
            throw defer("dispatcher_saturated", "dispatcher at its in-flight limit");
        }

        // From here on the permits are released exactly once: by the attempt, or below if it never starts.
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

        try {
            executor.execute(() -> runAttempt(record, message, endpoint, attempt, breaker, bulkhead, traceHeaders));
        } catch (RejectedExecutionException e) {
            releaseAll(breaker, bulkhead);
            deliveries.release(message.deliveryId(), "executor rejected the attempt");
            throw new RetryableDeliveryException("executor rejected the attempt");
        }
    }

    /** Runs on the delivery executor. Owns the permits and the delivery's lease until it returns. */
    private void runAttempt(ConsumerRecord<String, String> record, DeliveryMessage message, EndpointConfig endpoint,
            int attempt, CircuitBreaker breaker, Bulkhead bulkhead, Map<String, String> traceHeaders) {
        boolean called = false;
        try {
            if (resilience.rateLimitEnabled()
                    && !rateLimiter.acquire(endpoint.id(), endpoint.rateLimitPerSec(), resilience.rateLimitMaxWait())) {
                metrics.skipped("rate_limited");
                if (forward(record, new RetryableDeliveryException(
                        "rate limit of " + endpoint.rateLimitPerSec() + "/s reached"))) {
                    deliveries.release(message.deliveryId(), "rate limited");
                }
                return;
            }
            byte[] body = message.toWireBody().getBytes(StandardCharsets.UTF_8);
            HttpOutcome outcome = sender.send(endpoint, message.webhookId().toString(), body, traceHeaders);
            called = true;
            recordInBreaker(breaker, outcome);
            metrics.attempt(attempt, outcome);
            switch (outcome.kind()) {
                case SUCCESS -> deliveries.recordAttempt(message.deliveryId(), message.endpointId(), attempt, outcome, false);
                case PERMANENT -> {
                    forward(record, new PermanentDeliveryException(outcome.error()));
                    deliveries.recordAttempt(message.deliveryId(), message.endpointId(), attempt, outcome, false);
                }
                default -> {
                    boolean scheduled = forward(record, new RetryableDeliveryException(outcome.error(), outcome.retryAfter()));
                    deliveries.recordAttempt(message.deliveryId(), message.endpointId(), attempt, outcome, scheduled);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            // E.g. the database is down while recording. The lease expires and the sweeper re-enqueues it.
            log.warn("Attempt {} for delivery {} failed {} the HTTP call: {}", attempt, message.deliveryId(),
                    called ? "after" : "before", e.toString());
        } finally {
            if (!called) {
                breaker.releasePermission();
            }
            bulkhead.onComplete();
            inFlight.release();
        }
    }

    /** Returns false (and logs) if the record could not be published; the lease then covers it. */
    private boolean forward(ConsumerRecord<String, String> record, RuntimeException cause) {
        try {
            forwarder.forward(record, cause);
            return true;
        } catch (RuntimeException e) {
            log.warn("Could not forward {}-{}@{} to its retry topic; the lease sweeper will re-enqueue it: {}",
                    record.topic(), record.partition(), record.offset(), e.toString());
            return false;
        }
    }

    private boolean acquireInFlight() {
        try {
            return inFlight.tryAcquire(IN_FLIGHT_WAIT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
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

    private RetryableDeliveryException defer(String metric, String reason) {
        metrics.skipped(metric);
        return new RetryableDeliveryException(reason);
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
        // IN_FLIGHT under a live lease: another copy of this delivery is being attempted right now.
        // Come back when the lease runs out, not on the next short retry delay.
        metrics.skipped("in_flight_elsewhere");
        throw new RetryableDeliveryException("delivery is in flight on another worker",
                state.get().leaseRemaining().plusMillis(250));
    }
}
