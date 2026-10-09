package io.github.gouranshul.webhookrelay.api.delivery;

import io.github.gouranshul.webhookrelay.api.delivery.DeliveryDtos.BulkReplayRequest;
import io.github.gouranshul.webhookrelay.api.delivery.DeliveryDtos.DeliveryDetail;
import io.github.gouranshul.webhookrelay.api.delivery.DeliveryDtos.DeliverySummary;
import io.github.gouranshul.webhookrelay.api.delivery.DeliveryDtos.Page;
import io.github.gouranshul.webhookrelay.api.delivery.DeliveryDtos.ReplayResult;
import io.github.gouranshul.webhookrelay.common.model.DeliveryStatus;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/deliveries")
public class DeliveryController {

    private final DeliveryService service;

    public DeliveryController(DeliveryService service) {
        this.service = service;
    }

    /** Newest first, cursor-paginated. All filters are optional. */
    @GetMapping
    public Page<DeliverySummary> list(
            @RequestParam(required = false) UUID endpointId,
            @RequestParam(required = false) DeliveryStatus status,
            @RequestParam(required = false) UUID eventId,
            @RequestParam(required = false) String cursor,
            @RequestParam(defaultValue = "50") int limit) {
        return service.list(endpointId, status, eventId, cursor, limit);
    }

    /** The delivery, every attempt (status code, latency, truncated error), and any replays of it. */
    @GetMapping("/{id}")
    public DeliveryDetail get(@PathVariable UUID id) {
        return service.get(id);
    }

    /** Sends a finished delivery again, as a new delivery with the same Webhook-Id. */
    @PostMapping("/{id}/replay")
    public ResponseEntity<DeliverySummary> replay(@PathVariable UUID id) {
        DeliverySummary created = service.replay(id);
        return ResponseEntity.accepted().location(URI.create("/v1/deliveries/" + created.id())).body(created);
    }

    /** Replays an endpoint's dead-lettered deliveries in a time range, oldest first, each at most once. */
    @PostMapping("/replay")
    public ResponseEntity<ReplayResult> bulkReplay(@Valid @RequestBody BulkReplayRequest request) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(service.bulkReplay(request));
    }
}
