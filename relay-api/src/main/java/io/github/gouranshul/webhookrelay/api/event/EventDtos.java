package io.github.gouranshul.webhookrelay.api.event;

import io.github.gouranshul.webhookrelay.api.delivery.DeliveryDtos.DeliverySummary;
import io.github.gouranshul.webhookrelay.api.endpoint.EndpointDtos;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import tools.jackson.databind.JsonNode;

public final class EventDtos {

    private EventDtos() {
    }

    public record PublishEventRequest(
            @NotBlank @Size(max = 100) @Pattern(regexp = EndpointDtos.EVENT_TYPE_PATTERN) String type,
            @NotNull JsonNode data) {
    }

    public record EventView(UUID id, String type, JsonNode data, Instant createdAt,
            List<DeliverySummary> deliveries) {
    }

    /** Response to {@code POST /v1/events}; also what an idempotent replay returns. */
    public record EventAccepted(UUID id, String type, int deliveries, Instant createdAt) {
    }
}
