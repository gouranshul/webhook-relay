package io.github.gouranshul.webhookrelay.api.event;

import io.github.gouranshul.webhookrelay.api.config.RelayApiProperties;
import io.github.gouranshul.webhookrelay.api.event.EventDtos.EventAccepted;
import io.github.gouranshul.webhookrelay.api.event.EventDtos.PublishEventRequest;
import io.github.gouranshul.webhookrelay.api.event.IdempotencyRepository.StoredResponse;
import io.github.gouranshul.webhookrelay.api.web.ApiException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/**
 * Accepts an event and, in ONE transaction, writes the event, a delivery and an outbox row per subscribed
 * endpoint, and the idempotency key with its response. Either all of it commits or none of it does.
 */
@Service
public class EventService {

    public record Result(EventAccepted response, boolean replayed) {
    }

    private final EventRepository events;
    private final IdempotencyRepository idempotency;
    private final ObjectMapper mapper;
    private final RelayApiProperties properties;

    public EventService(EventRepository events, IdempotencyRepository idempotency, ObjectMapper mapper,
            RelayApiProperties properties) {
        this.events = events;
        this.idempotency = idempotency;
        this.mapper = mapper;
        this.properties = properties;
    }

    @Transactional
    public Result publish(String idempotencyKey, PublishEventRequest request, String traceparent) {
        String payload = request.data().toString();
        if (payload.getBytes(StandardCharsets.UTF_8).length > properties.events().maxPayloadBytes()) {
            throw new ApiException(HttpStatus.CONTENT_TOO_LARGE,
                    "data exceeds " + properties.events().maxPayloadBytes() + " bytes");
        }
        if (!request.data().isObject()) {
            throw ApiException.badRequest("data must be a JSON object");
        }

        if (idempotencyKey != null) {
            String hash = hash(request.type(), payload);
            if (!idempotency.claim(idempotencyKey, hash, properties.events().idempotencyTtl())) {
                StoredResponse stored = idempotency.find(idempotencyKey)
                        .orElseThrow(() -> ApiException.conflict("Idempotency key is being processed; retry"));
                if (!stored.requestHash().equals(hash)) {
                    throw ApiException.unprocessable("Idempotency-Key was already used with a different request");
                }
                return new Result(mapper.readValue(stored.body(), EventAccepted.class), true);
            }
        }

        UUID eventId = UUID.randomUUID();
        Instant createdAt = events.insert(eventId, request.type(), payload, traceparent);
        int deliveries = events.fanOut(eventId, request.type());
        EventAccepted response = new EventAccepted(eventId, request.type(), deliveries, createdAt);

        if (idempotencyKey != null) {
            idempotency.complete(idempotencyKey, HttpStatus.ACCEPTED.value(), mapper.writeValueAsString(response));
        }
        return new Result(response, false);
    }

    static String hash(String type, String payload) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(type.getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            digest.update(payload.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
