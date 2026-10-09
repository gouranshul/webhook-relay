package io.github.gouranshul.webhookrelay.api.endpoint;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.github.gouranshul.webhookrelay.common.model.EndpointStatus;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.List;

public final class EndpointDtos {

    /** An event type such as {@code order.created}, or {@code *} for all types. */
    public static final String EVENT_TYPE_PATTERN = "^(\\*|[a-z][a-z0-9_]*(\\.[a-z0-9_]+)+)$";

    private EndpointDtos() {
    }

    public record CreateEndpointRequest(
            @NotBlank @Size(max = 2048) String url,
            @Size(max = 500) String description,
            @NotEmpty @Size(max = 50) List<@NotBlank @Size(max = 100) @Pattern(regexp = EVENT_TYPE_PATTERN) String> eventTypes,
            @Min(1) @Max(10_000) Integer rateLimitPerSec) {
    }

    /** Partial update. Null fields are left unchanged. {@code status} may be ACTIVE or PAUSED. */
    public record UpdateEndpointRequest(
            @Size(max = 2048) String url,
            @Size(max = 500) String description,
            @Size(min = 1, max = 50) List<@NotBlank @Size(max = 100) @Pattern(regexp = EVENT_TYPE_PATTERN) String> eventTypes,
            @Min(1) @Max(10_000) Integer rateLimitPerSec,
            EndpointStatus status) {
    }

    /** Returned once, on creation. The secret is never retrievable again. */
    public record CreatedEndpoint(@JsonUnwrapped Endpoint endpoint, String secret) {
    }
}
