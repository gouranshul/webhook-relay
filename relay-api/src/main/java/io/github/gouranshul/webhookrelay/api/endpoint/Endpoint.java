package io.github.gouranshul.webhookrelay.api.endpoint;

import io.github.gouranshul.webhookrelay.common.model.EndpointStatus;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** An endpoint as returned by the API. Never carries the signing secret. */
public record Endpoint(
        UUID id,
        String url,
        String description,
        List<String> eventTypes,
        int rateLimitPerSec,
        EndpointStatus status,
        int consecutiveDlt,
        Instant createdAt,
        Instant updatedAt) {
}
