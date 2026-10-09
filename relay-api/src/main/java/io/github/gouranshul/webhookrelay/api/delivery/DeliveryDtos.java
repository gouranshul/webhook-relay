package io.github.gouranshul.webhookrelay.api.delivery;

import io.github.gouranshul.webhookrelay.common.model.DeliveryStatus;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public final class DeliveryDtos {

    private DeliveryDtos() {
    }

    public record DeliverySummary(
            UUID id,
            UUID webhookId,
            UUID eventId,
            String eventType,
            UUID endpointId,
            DeliveryStatus status,
            int attemptCount,
            Integer lastStatusCode,
            String lastError,
            UUID replayOf,
            Instant createdAt,
            Instant updatedAt,
            Instant completedAt,
            Instant deadLetteredAt) {
    }

    public record AttemptView(int attempt, String outcome, Integer statusCode, int latencyMs, String error,
            Instant attemptedAt) {
    }

    public record DeliveryDetail(DeliverySummary delivery, List<AttemptView> attempts, List<UUID> replays) {
    }

    public record Page<T>(List<T> data, String nextCursor) {
    }

    /** Re-delivers dead-lettered deliveries of one endpoint whose DLT time is in [from, to). */
    public record BulkReplayRequest(
            @NotNull UUID endpointId,
            @NotNull Instant from,
            @NotNull Instant to,
            @Min(1) @Max(10_000) Integer limit) {
    }

    public record ReplayResult(int replayed, List<UUID> deliveryIds) {
    }
}
