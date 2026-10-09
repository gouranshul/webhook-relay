package io.github.gouranshul.webhookrelay.dispatcher.delivery;

import io.github.gouranshul.webhookrelay.common.model.DeliveryMessage;
import io.github.gouranshul.webhookrelay.dispatcher.endpoint.EndpointHealth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Records that a delivery reached the DLT. Idempotent: a redelivered DLT record changes nothing. */
@Component
public class DeadLetterHandler {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterHandler.class);
    private static final int MAX_REASON = 512;

    private final DeliveryRepository deliveries;
    private final DeliveryMetrics metrics;
    private final EndpointHealth endpointHealth;

    public DeadLetterHandler(DeliveryRepository deliveries, DeliveryMetrics metrics, EndpointHealth endpointHealth) {
        this.deliveries = deliveries;
        this.metrics = metrics;
        this.endpointHealth = endpointHealth;
    }

    public void handle(DeliveryMessage message, String reason) {
        String trimmed = reason == null ? "dead-lettered"
                : reason.length() > MAX_REASON ? reason.substring(0, MAX_REASON) : reason;
        if (deliveries.markDeadLettered(message.deliveryId(), trimmed)) {
            metrics.deadLettered();
            log.info("Delivery {} to endpoint {} dead-lettered: {}", message.deliveryId(), message.endpointId(), trimmed);
            endpointHealth.onDeadLettered(message.endpointId());
        }
    }
}
