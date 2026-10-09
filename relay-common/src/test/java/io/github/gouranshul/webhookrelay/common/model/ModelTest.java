package io.github.gouranshul.webhookrelay.common.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ModelTest {

    @Test
    void wireBodyEmbedsPayloadVerbatim() {
        UUID eventId = UUID.fromString("00000000-0000-0000-0000-000000000001");
        var message = new DeliveryMessage(UUID.randomUUID(), UUID.randomUUID(), eventId, UUID.randomUUID(),
                "order.created", "{\"order_id\": 42}", Instant.parse("2026-01-01T00:00:00Z"));
        assertThat(message.toWireBody()).isEqualTo(
                "{\"id\":\"00000000-0000-0000-0000-000000000001\",\"type\":\"order.created\","
                        + "\"created_at\":\"2026-01-01T00:00:00Z\",\"data\":{\"order_id\": 42}}");
    }

    @Test
    void terminalStates() {
        assertThat(DeliveryStatus.SUCCEEDED.isTerminal()).isTrue();
        assertThat(DeliveryStatus.FAILED.isTerminal()).isTrue();
        assertThat(DeliveryStatus.CANCELLED.isTerminal()).isTrue();
        assertThat(DeliveryStatus.RETRYING.isTerminal()).isFalse();
        assertThat(DeliveryStatus.PARKED.isTerminal()).isFalse();
    }
}
