package io.github.gouranshul.webhookrelay.receiver;

import io.github.gouranshul.webhookrelay.common.signing.WebhookHeaders;
import io.github.gouranshul.webhookrelay.common.signing.WebhookVerifier;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * The receiving end. Behaves like a real consumer would: verifies the signature (when it knows the secret),
 * dedupes on {@code Webhook-Id}, and otherwise fails and stalls as configured.
 */
@RestController
public class HookController {

    private final ReceiverState state;
    private final WebhookVerifier verifier = new WebhookVerifier();
    private final MeterRegistry registry;

    public HookController(ReceiverState state, MeterRegistry registry) {
        this.state = state;
        this.registry = registry;
    }

    @PostMapping("/hooks/{name}")
    public ResponseEntity<String> receive(
            @PathVariable String name,
            @RequestHeader(name = WebhookHeaders.ID, required = false) String webhookId,
            @RequestHeader(name = WebhookHeaders.TIMESTAMP, required = false) String timestamp,
            @RequestHeader(name = WebhookHeaders.SIGNATURE, required = false) String signature,
            @RequestHeader(name = "traceparent", required = false) String traceparent,
            @RequestBody(required = false) byte[] body) throws InterruptedException {
        Instant receivedAt = Instant.now();
        HookState hook = state.hook(name);
        hook.onRequest();
        Behavior behavior = state.behaviorOf(hook);

        String signatureResult = "unchecked";
        if (behavior.secret() != null) {
            WebhookVerifier.Result result = verifier.verify(behavior.secret(), timestamp, signature,
                    body == null ? new byte[0] : body);
            signatureResult = result.name();
            if (!result.isValid()) {
                hook.onSignatureFailure();
                return respond(hook, webhookId, receivedAt, 401, signatureResult, traceparent, behavior);
            }
        }

        long latency = behavior.maxLatencyMs() == 0 ? 0
                : ThreadLocalRandom.current().nextLong(behavior.minLatencyMs(), behavior.maxLatencyMs() + 1);
        if (latency > 0) {
            Thread.sleep(latency);
        }

        Integer scripted = hook.nextScripted();
        int status;
        if (scripted != null) {
            status = scripted;
        } else if (behavior.failureRate() > 0 && ThreadLocalRandom.current().nextDouble() < behavior.failureRate()) {
            status = behavior.failureStatus();
        } else {
            status = 200;
        }
        return respond(hook, webhookId, receivedAt, status, signatureResult, traceparent, behavior);
    }

    private ResponseEntity<String> respond(HookState hook, String webhookId, Instant receivedAt, int status,
            String signatureResult, String traceparent, Behavior behavior) {
        String outcome;
        if (status >= 200 && status < 300) {
            outcome = webhookId != null && hook.onSuccess(webhookId) ? "duplicate" : "success";
        } else {
            hook.onFailureReturned();
            outcome = "failure";
        }
        hook.record(new HookState.Received(webhookId, receivedAt, status, signatureResult, traceparent));
        registry.counter("receiver.requests", "outcome", outcome).increment();

        ResponseEntity.BodyBuilder response = ResponseEntity.status(status);
        if (status >= 400 && behavior.retryAfterSeconds() != null) {
            response.header("Retry-After", Integer.toString(behavior.retryAfterSeconds()));
        }
        return response.body(status < 300 ? "ok" : "simulated failure");
    }
}
