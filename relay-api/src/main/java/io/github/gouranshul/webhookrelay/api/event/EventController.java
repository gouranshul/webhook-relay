package io.github.gouranshul.webhookrelay.api.event;

import io.github.gouranshul.webhookrelay.api.delivery.DeliveryService;
import io.github.gouranshul.webhookrelay.api.event.EventDtos.EventAccepted;
import io.github.gouranshul.webhookrelay.api.event.EventDtos.PublishEventRequest;
import io.github.gouranshul.webhookrelay.api.tracing.TraceContext;
import io.github.gouranshul.webhookrelay.api.web.ApiException;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/events")
public class EventController {

    public static final String IDEMPOTENCY_KEY = "Idempotency-Key";
    public static final String IDEMPOTENT_REPLAYED = "Idempotent-Replayed";

    private final EventService service;
    private final TraceContext traceContext;
    private final DeliveryService deliveries;

    public EventController(EventService service, TraceContext traceContext,
            DeliveryService deliveries) {
        this.service = service;
        this.traceContext = traceContext;
        this.deliveries = deliveries;
    }

    /** The event and its deliveries (first 200), one per subscribed endpoint plus any replays. */
    @GetMapping("/{id}")
    public EventDtos.EventView get(@PathVariable UUID id) {
        return service.get(id, deliveries.list(null, null, id, null, 200).data());
    }

    /**
     * Accepts an event for delivery to every subscribed endpoint. Returns 202 once the event and its outbox
     * rows are committed; delivery happens asynchronously. Repeating a request with the same
     * {@code Idempotency-Key} within 24h returns the original response and creates nothing.
     */
    @PostMapping
    public ResponseEntity<EventAccepted> publish(
            @RequestHeader(value = IDEMPOTENCY_KEY, required = false) String idempotencyKey,
            @Valid @RequestBody PublishEventRequest request) {
        if (idempotencyKey != null && (idempotencyKey.isBlank() || idempotencyKey.length() > 255)) {
            throw ApiException.badRequest("Idempotency-Key must be 1-255 characters");
        }
        EventService.Result result = service.publish(idempotencyKey, request, traceContext.currentTraceparent());
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .location(URI.create("/v1/events/" + result.response().id()))
                .header(IDEMPOTENT_REPLAYED, Boolean.toString(result.replayed()))
                .body(result.response());
    }
}
