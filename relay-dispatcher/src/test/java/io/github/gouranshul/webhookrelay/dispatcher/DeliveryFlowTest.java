package io.github.gouranshul.webhookrelay.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.gouranshul.webhookrelay.common.model.DeliveryMessage;
import io.github.gouranshul.webhookrelay.common.model.DeliveryStatus;
import io.github.gouranshul.webhookrelay.receiver.Behavior;
import io.github.gouranshul.webhookrelay.receiver.HookState;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class DeliveryFlowTest extends DispatcherIntegrationTest {

    @Test
    void deliversSignedRequestOnFirstAttempt() {
        TestEndpoint endpoint = endpoint(Behavior.healthy());
        DeliveryMessage message = enqueue(endpoint);

        awaitStatus(message.deliveryId(), DeliveryStatus.SUCCEEDED, Duration.ofSeconds(20));

        assertThat(attempts(message.deliveryId())).singleElement().satisfies(a -> {
            assertThat(a.number()).isEqualTo(1);
            assertThat(a.outcome()).isEqualTo("SUCCESS");
            assertThat(a.statusCode()).isEqualTo(200);
            assertThat(a.error()).isNull();
        });
        List<HookState.Received> received = hook(endpoint).recent();
        assertThat(received).singleElement().satisfies(r -> {
            assertThat(r.signature()).isEqualTo("VALID");
            assertThat(r.webhookId()).isEqualTo(message.webhookId().toString());
        });
    }

    @Test
    void retriesRetryableStatusesThenSucceeds() {
        TestEndpoint endpoint = endpoint(scripted(503, 500));
        DeliveryMessage message = enqueue(endpoint);

        awaitStatus(message.deliveryId(), DeliveryStatus.SUCCEEDED, Duration.ofSeconds(30));

        assertThat(attempts(message.deliveryId()))
                .extracting(Attempt::number, Attempt::outcome, Attempt::statusCode)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(1, "RETRYABLE", 503),
                        org.assertj.core.groups.Tuple.tuple(2, "RETRYABLE", 500),
                        org.assertj.core.groups.Tuple.tuple(3, "SUCCESS", 200));
        assertThat(attempts(message.deliveryId()).get(0).error()).startsWith("HTTP 503: simulated failure");
        assertThat(hook(endpoint).stats().duplicates()).isZero();
    }

    @Test
    void treats408And429AsRetryable() {
        TestEndpoint endpoint = endpoint(scripted(408, 429));
        DeliveryMessage message = enqueue(endpoint);

        awaitStatus(message.deliveryId(), DeliveryStatus.SUCCEEDED, Duration.ofSeconds(30));

        assertThat(attempts(message.deliveryId())).extracting(Attempt::statusCode).containsExactly(408, 429, 200);
    }

    @Test
    void permanentFailureGoesStraightToDeadLetterTopic() {
        TestEndpoint endpoint = endpoint(scripted(400));
        DeliveryMessage message = enqueue(endpoint);

        awaitStatus(message.deliveryId(), DeliveryStatus.FAILED, Duration.ofSeconds(20));
        await().atMost(Duration.ofSeconds(20)).until(() -> deadLettered(message.deliveryId()));

        assertThat(attempts(message.deliveryId())).singleElement()
                .satisfies(a -> assertThat(a.outcome()).isEqualTo("PERMANENT"));
        assertThat(hook(endpoint).stats().requests()).isEqualTo(1);
    }

    @Test
    void exhaustedRetriesEndOnDeadLetterTopic() {
        TestEndpoint endpoint = endpoint(new Behavior(1.0, 0, 0, 503, null, null, null));
        DeliveryMessage message = enqueue(endpoint);

        await().atMost(Duration.ofSeconds(40)).until(() -> deadLettered(message.deliveryId()));

        assertThat(status(message.deliveryId())).isEqualTo(DeliveryStatus.FAILED);
        assertThat(attempts(message.deliveryId())).hasSize(4).allSatisfy(a -> assertThat(a.statusCode()).isEqualTo(503));
        // Delays grow: ~300ms, ~600ms, ~1200ms (+ up to 20% jitter each).
        List<Attempt> attempts = attempts(message.deliveryId());
        Duration first = Duration.between(attempts.get(0).at(), attempts.get(1).at());
        Duration third = Duration.between(attempts.get(2).at(), attempts.get(3).at());
        assertThat(first).isGreaterThanOrEqualTo(Duration.ofMillis(280));
        assertThat(third).isGreaterThanOrEqualTo(Duration.ofMillis(1150)).isGreaterThan(first);
    }

    @Test
    void wrongSignatureIsRejectedByReceiverAndNotRetried() {
        TestEndpoint endpoint = endpoint(Behavior.healthy());
        RECEIVER.state().configure(endpoint.hook(), Behavior.healthy().withSecret("whsec_not_the_real_one"));
        DeliveryMessage message = enqueue(endpoint);

        awaitStatus(message.deliveryId(), DeliveryStatus.FAILED, Duration.ofSeconds(20));

        assertThat(attempts(message.deliveryId())).singleElement()
                .satisfies(a -> assertThat(a.statusCode()).isEqualTo(401));
        assertThat(hook(endpoint).stats().signatureFailures()).isEqualTo(1);
    }

    @Test
    void timesOutSlowReceiversAndRetries() {
        TestEndpoint endpoint = endpoint(new Behavior(0, 1500, 1500, 503, null, null, null));
        DeliveryMessage message = enqueue(endpoint);

        await().atMost(Duration.ofSeconds(40)).until(() -> deadLettered(message.deliveryId()));

        assertThat(attempts(message.deliveryId())).hasSize(4).allSatisfy(a -> {
            assertThat(a.outcome()).isEqualTo("TIMEOUT");
            assertThat(a.statusCode()).isNull();
            assertThat(a.latencyMs()).isBetween(900, 1500);
        });
    }

    @Test
    void honoursRetryAfter() {
        TestEndpoint endpoint = endpoint(new Behavior(0, 0, 0, 503, 2, List.of(429), null));
        DeliveryMessage message = enqueue(endpoint);

        awaitStatus(message.deliveryId(), DeliveryStatus.SUCCEEDED, Duration.ofSeconds(20));

        List<Attempt> attempts = attempts(message.deliveryId());
        assertThat(attempts).hasSize(2);
        // Without Retry-After the gap would be ~300-360 ms.
        assertThat(Duration.between(attempts.get(0).at(), attempts.get(1).at()))
                .isGreaterThanOrEqualTo(Duration.ofMillis(1900));
    }

    @Test
    void redeliveredRecordsNeverCauseASecondSend() {
        TestEndpoint endpoint = endpoint(new Behavior(0, 200, 200, 503, null, null, null));
        DeliveryMessage message = insertDelivery(endpoint, "user.signed_up", "{\"user\":\"u-1\"}");

        // The same record three times: as an outbox republish after a lost commit, or a rebalance, would.
        publish(message, null);
        publish(message, null);
        awaitStatus(message.deliveryId(), DeliveryStatus.SUCCEEDED, Duration.ofSeconds(20));
        publish(message, null);

        // Give every copy (including ones parked on retry topics) time to be consumed.
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10))
                .until(() -> hook(endpoint).stats().requests() == 1);
        assertThat(hook(endpoint).stats().duplicates()).isZero();
        assertThat(attempts(message.deliveryId())).hasSize(1);
    }

    @Test
    void pausedEndpointParksAndDeletedEndpointCancels() {
        TestEndpoint paused = endpoint(Behavior.healthy());
        jdbc.sql("UPDATE endpoints SET status = 'PAUSED' WHERE id = ?").param(paused.id()).update();
        TestEndpoint deleted = endpoint(Behavior.healthy());
        jdbc.sql("UPDATE endpoints SET status = 'DELETED' WHERE id = ?").param(deleted.id()).update();

        DeliveryMessage parked = enqueue(paused);
        DeliveryMessage cancelled = enqueue(deleted);

        awaitStatus(parked.deliveryId(), DeliveryStatus.PARKED, Duration.ofSeconds(20));
        awaitStatus(cancelled.deliveryId(), DeliveryStatus.CANCELLED, Duration.ofSeconds(20));
        assertThat(hook(paused).stats().requests()).isZero();
        assertThat(hook(deleted).stats().requests()).isZero();
    }
}
