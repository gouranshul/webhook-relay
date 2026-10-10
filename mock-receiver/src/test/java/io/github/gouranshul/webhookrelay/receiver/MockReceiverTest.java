package io.github.gouranshul.webhookrelay.receiver;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gouranshul.webhookrelay.common.signing.WebhookSigner;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

@SpringBootTest(properties = MockReceiverApplication.CONFIG_NAME)
@AutoConfigureMockMvc
class MockReceiverTest {

    private static final String SECRET = "whsec_receiver_test";

    @Autowired
    MockMvcTester mvc;
    @Autowired
    ReceiverState state;

    private MvcTestResult deliver(String hook, String webhookId, String secret, long timestamp) {
        byte[] body = "{\"id\":\"e1\",\"type\":\"order.created\",\"data\":{}}".getBytes(StandardCharsets.UTF_8);
        return mvc.post().uri("/hooks/" + hook)
                .contentType(MediaType.APPLICATION_JSON)
                .header("Webhook-Id", webhookId)
                .header("Webhook-Timestamp", Long.toString(timestamp))
                .header("Webhook-Signature", new WebhookSigner().sign(secret, timestamp, body))
                .content(body)
                .exchange();
    }

    private static long now() {
        return Instant.now().getEpochSecond();
    }

    @Test
    void verifiesSignaturesWhenSecretIsKnown() {
        state.configure("signed", Behavior.healthy().withSecret(SECRET));

        assertThat(deliver("signed", "w1", SECRET, now())).hasStatusOk();
        assertThat(deliver("signed", "w2", "whsec_wrong", now())).hasStatus(401);
        assertThat(deliver("signed", "w3", SECRET, now() - 600)).hasStatus(401);

        HookState hook = state.hook("signed");
        assertThat(hook.stats().signatureFailures()).isEqualTo(2);
        assertThat(hook.recent()).extracting(HookState.Received::signature)
                .containsExactly("VALID", "INVALID_SIGNATURE", "TIMESTAMP_TOO_OLD");
    }

    @Test
    void followsScriptThenFailureRateAndSendsRetryAfter() {
        state.configure("scripted", new Behavior(1.0, 0, 0, 429, 7, List.of(500, 200), null));

        assertThat(deliver("scripted", "a", SECRET, now())).hasStatus(500)
                .headers().hasValue("Retry-After", "7");
        assertThat(deliver("scripted", "b", SECRET, now())).hasStatusOk();
        assertThat(deliver("scripted", "c", SECRET, now())).hasStatus(429);
    }

    @Test
    void unconfiguredHookUsesDefaultsInsteadOfFailing() {
        assertThat(deliver("never-configured-" + System.nanoTime(), "z", SECRET, now())).hasStatusOk();
    }

    @Test
    void countsDuplicateSuccessfulDeliveries() {
        state.configure("dedupe", Behavior.healthy());
        deliver("dedupe", "same-id", SECRET, now());
        deliver("dedupe", "same-id", SECRET, now());
        deliver("dedupe", "other-id", SECRET, now());

        HookState.Stats stats = state.hook("dedupe").stats();
        assertThat(stats.requests()).isEqualTo(3);
        assertThat(stats.uniqueSucceeded()).isEqualTo(2);
        assertThat(stats.duplicates()).isEqualTo(1);
    }

    @Test
    void adminApiConfiguresAndReports() {
        assertThat(mvc.put().uri("/admin/hooks/admin-hook").contentType(MediaType.APPLICATION_JSON)
                .content("{\"failureRate\":0,\"minLatencyMs\":0,\"maxLatencyMs\":0,\"failureStatus\":503}")
                .exchange()).hasStatusOk();
        deliver("admin-hook", "x", SECRET, now());
        assertThat(mvc.get().uri("/admin/hooks/admin-hook/stats").exchange())
                .bodyJson().extractingPath("$.uniqueSucceeded").isEqualTo(1);
        assertThat(mvc.get().uri("/admin/stats").exchange()).hasStatusOk();
        assertThat(mvc.delete().uri("/admin/hooks/admin-hook").exchange()).hasStatus(204);
    }
}
