package io.github.gouranshul.webhookrelay.dispatcher.delivery;

import io.github.gouranshul.webhookrelay.common.model.DeliveryMessage;
import io.github.gouranshul.webhookrelay.common.model.DeliveryStatus;
import io.github.gouranshul.webhookrelay.dispatcher.config.DispatcherProperties;
import io.github.gouranshul.webhookrelay.dispatcher.endpoint.EndpointCache;
import io.github.gouranshul.webhookrelay.dispatcher.endpoint.EndpointConfig;
import io.github.gouranshul.webhookrelay.dispatcher.http.HttpOutcome;
import io.github.gouranshul.webhookrelay.dispatcher.http.WebhookSender;
import io.github.gouranshul.webhookrelay.dispatcher.tracing.OutboundTracing;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Delivers one message: resolve the endpoint, claim the delivery row (the idempotency check), send, record.
 *
 * <p>Returns normally when the record needs no further work (delivered, or a duplicate of something already
 * finished). Throws {@link RetryableDeliveryException} to schedule a retry and {@link PermanentDeliveryException}
 * to dead-letter.
 */
@Component
public class DeliveryProcessor {

    private static final Logger log = LoggerFactory.getLogger(DeliveryProcessor.class);

    private final EndpointCache endpoints;
    private final DeliveryRepository deliveries;
    private final WebhookSender sender;
    private final DeliveryMetrics metrics;
    private final OutboundTracing tracing;
    private final Duration lease;

    public DeliveryProcessor(EndpointCache endpoints, DeliveryRepository deliveries, WebhookSender sender,
            DeliveryMetrics metrics, OutboundTracing tracing, DispatcherProperties properties) {
        this.endpoints = endpoints;
        this.deliveries = deliveries;
        this.sender = sender;
        this.metrics = metrics;
        this.tracing = tracing;
        this.lease = properties.dispatcher().requestTimeout().plus(properties.dispatcher().leaseMargin());
    }

    public void process(DeliveryMessage message) {
        EndpointConfig endpoint = endpoints.get(message.endpointId());
        if (!isDeliverable(message, endpoint)) {
            return;
        }

        OptionalInt claimed = deliveries.claim(message.deliveryId(), lease);
        if (claimed.isEmpty()) {
            handleUnclaimable(message);
            return;
        }
        int attempt = claimed.getAsInt();

        byte[] body = message.toWireBody().getBytes(StandardCharsets.UTF_8);
        HttpOutcome outcome = sender.send(endpoint, message.webhookId().toString(), body, tracing.currentHeaders());
        deliveries.recordAttempt(message.deliveryId(), message.endpointId(), attempt, outcome);
        metrics.attempt(attempt, outcome);

        switch (outcome.kind()) {
            case SUCCESS -> {
            }
            case PERMANENT -> throw new PermanentDeliveryException(outcome.error());
            default -> throw new RetryableDeliveryException(outcome.error(), outcome.retryAfter());
        }
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
        Optional<DeliveryStatus> status = deliveries.status(message.deliveryId());
        if (status.isEmpty()) {
            log.warn("Delivery {} not found; dropping record", message.deliveryId());
            metrics.skipped("unknown_delivery");
            return;
        }
        if (status.get().isTerminal() || status.get() == DeliveryStatus.PARKED) {
            // Redelivered record (outbox republish, rebalance, retry copy of a finished delivery): no second send.
            metrics.skipped("duplicate");
            return;
        }
        // IN_FLIGHT under another worker's live lease. Check again on the next retry.
        metrics.skipped("in_flight_elsewhere");
        throw new RetryableDeliveryException("delivery is in flight on another worker");
    }
}
