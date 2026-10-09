package io.github.gouranshul.webhookrelay.dispatcher.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gouranshul.webhookrelay.common.model.DeliveryMessage;
import io.github.gouranshul.webhookrelay.common.model.DeliveryStatus;
import io.github.gouranshul.webhookrelay.dispatcher.DispatcherIntegrationTest;
import io.github.gouranshul.webhookrelay.receiver.Behavior;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * A dispatcher that dies mid-attempt leaves the delivery IN_FLIGHT, and its Kafka record was already
 * committed. The sweeper must notice the expired lease and re-enqueue the delivery through the outbox.
 */
class LeaseSweeperTest extends DispatcherIntegrationTest {

    @Autowired
    LeaseSweeper sweeper;

    @Test
    void reenqueuesDeliveriesWhoseLeaseExpired() {
        TestEndpoint endpoint = endpoint(Behavior.healthy());
        DeliveryMessage orphan = insertDelivery(endpoint, "order.created", "{\"o\":1}");
        DeliveryMessage live = insertDelivery(endpoint, "order.created", "{\"o\":2}");
        jdbc.sql("UPDATE deliveries SET status = 'IN_FLIGHT', attempt_count = 1, lease_until = now() - interval '1 second' WHERE id = ?")
                .param(orphan.deliveryId()).update();
        jdbc.sql("UPDATE deliveries SET status = 'IN_FLIGHT', attempt_count = 1, lease_until = now() + interval '1 minute' WHERE id = ?")
                .param(live.deliveryId()).update();

        sweeper.sweep();

        assertThat(status(orphan.deliveryId())).isEqualTo(DeliveryStatus.RETRYING);
        assertThat(status(live.deliveryId())).as("a live lease is left alone").isEqualTo(DeliveryStatus.IN_FLIGHT);
        assertThat(jdbc.sql("SELECT count(*) FROM outbox WHERE delivery_id = ? AND published_at IS NULL")
                .param(orphan.deliveryId()).query(Long.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT count(*) FROM outbox WHERE delivery_id = ?")
                .param(live.deliveryId()).query(Long.class).single()).isZero();
    }
}
