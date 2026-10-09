package io.github.gouranshul.webhookrelay.api.delivery;

import io.github.gouranshul.webhookrelay.api.delivery.DeliveryDtos.BulkReplayRequest;
import io.github.gouranshul.webhookrelay.api.delivery.DeliveryDtos.DeliveryDetail;
import io.github.gouranshul.webhookrelay.api.delivery.DeliveryDtos.DeliverySummary;
import io.github.gouranshul.webhookrelay.api.delivery.DeliveryDtos.Page;
import io.github.gouranshul.webhookrelay.api.delivery.DeliveryDtos.ReplayResult;
import io.github.gouranshul.webhookrelay.api.web.ApiException;
import io.github.gouranshul.webhookrelay.common.model.DeliveryStatus;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DeliveryService {

    private static final Duration MAX_REPLAY_WINDOW = Duration.ofDays(31);

    private final DeliveryQueryRepository repository;

    public DeliveryService(DeliveryQueryRepository repository) {
        this.repository = repository;
    }

    @Transactional(readOnly = true)
    public Page<DeliverySummary> list(UUID endpointId, DeliveryStatus status, UUID eventId, String cursor, int limit) {
        int size = Math.clamp(limit, 1, 200);
        Instant afterCreatedAt = null;
        UUID afterId = null;
        if (cursor != null && !cursor.isBlank()) {
            String[] parts = decode(cursor);
            afterCreatedAt = Instant.parse(parts[0]);
            afterId = UUID.fromString(parts[1]);
        }
        List<DeliverySummary> rows = repository.list(endpointId, status, eventId, afterCreatedAt, afterId, size + 1);
        String next = null;
        if (rows.size() > size) {
            rows = rows.subList(0, size);
            DeliverySummary last = rows.getLast();
            next = encode(last.createdAt() + "|" + last.id());
        }
        return new Page<>(rows, next);
    }

    @Transactional(readOnly = true)
    public DeliveryDetail get(UUID id) {
        DeliverySummary delivery = repository.find(id).orElseThrow(() -> ApiException.notFound("Delivery", id));
        return new DeliveryDetail(delivery, repository.attempts(id), repository.replaysOf(id));
    }

    @Transactional
    public DeliverySummary replay(UUID id) {
        DeliverySummary original = repository.find(id).orElseThrow(() -> ApiException.notFound("Delivery", id));
        if (!original.status().isTerminal() || original.status() == DeliveryStatus.CANCELLED) {
            throw ApiException.conflict("Only SUCCEEDED or FAILED deliveries can be replayed (status is "
                    + original.status() + ")");
        }
        UUID created = repository.replay(id)
                .orElseThrow(() -> ApiException.conflict("The endpoint for delivery " + id + " was deleted"));
        return repository.find(created).orElseThrow();
    }

    @Transactional
    public ReplayResult bulkReplay(BulkReplayRequest request) {
        if (!request.from().isBefore(request.to())) {
            throw ApiException.badRequest("from must be before to");
        }
        if (Duration.between(request.from(), request.to()).compareTo(MAX_REPLAY_WINDOW) > 0) {
            throw ApiException.badRequest("time range must not exceed 31 days");
        }
        if (!repository.endpointExists(request.endpointId())) {
            throw ApiException.notFound("Endpoint", request.endpointId());
        }
        int limit = request.limit() == null ? 1000 : request.limit();
        List<UUID> created = repository.bulkReplay(request.endpointId(), request.from(), request.to(), limit);
        return new ReplayResult(created.size(), created);
    }

    private static String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String[] decode(String cursor) {
        try {
            String[] parts = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8).split("\\|");
            if (parts.length != 2) {
                throw new IllegalArgumentException();
            }
            Instant.parse(parts[0]);
            UUID.fromString(parts[1]);
            return parts;
        } catch (RuntimeException e) {
            throw ApiException.badRequest("Invalid cursor");
        }
    }
}
