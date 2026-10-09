package io.github.gouranshul.webhookrelay.common.model;

import java.time.Instant;
import java.util.UUID;

/**
 * Kafka record value on {@code webhook.deliveries} and its retry topics. One message per (event, endpoint).
 *
 * @param deliveryId primary key of the delivery row; the dispatcher dedupes on it
 * @param webhookId  value of the {@code Webhook-Id} header; stable across retries and replays
 * @param eventId    the source event
 * @param endpointId the receiver; also the Kafka record key
 * @param eventType  e.g. {@code order.created}
 * @param payload    the event's {@code data} as raw JSON
 * @param occurredAt when the event was accepted by the API
 */
public record DeliveryMessage(
        UUID deliveryId,
        UUID webhookId,
        UUID eventId,
        UUID endpointId,
        String eventType,
        String payload,
        Instant occurredAt) {

    /**
     * The exact body POSTed to the receiver. Built by string concatenation so the event's JSON is sent
     * byte-for-byte as stored, and the signature covers precisely these bytes.
     */
    public String toWireBody() {
        return "{\"id\":\"" + eventId + "\",\"type\":\"" + eventType + "\",\"created_at\":\"" + occurredAt
                + "\",\"data\":" + payload + "}";
    }
}
