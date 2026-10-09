package io.github.gouranshul.webhookrelay.api.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gouranshul.webhookrelay.api.ApiIntegrationTest;
import io.github.gouranshul.webhookrelay.common.crypto.SecretCipher;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.assertj.MvcTestResult;
import tools.jackson.databind.JsonNode;

class EndpointApiTest extends ApiIntegrationTest {

    @Autowired
    SecretCipher cipher;

    @Test
    void createShowsSecretOnceAndStoresItEncrypted() {
        String type = uniqueType("order");
        MvcTestResult created = post("/v1/endpoints", """
                {"url":"http://receiver.local/hooks","eventTypes":["%s"],"rateLimitPerSec":25,"description":"orders"}
                """.formatted(type));
        assertThat(created).hasStatus(HttpStatus.CREATED);
        JsonNode body = json(created);
        String secret = body.get("secret").asString();
        UUID id = UUID.fromString(body.get("id").asString());
        assertThat(secret).startsWith("whsec_");
        assertThat(body.get("rateLimitPerSec").asInt()).isEqualTo(25);
        assertThat(body.get("status").asString()).isEqualTo("ACTIVE");
        assertThat(created.getResponse().getHeader("Location")).isEqualTo("/v1/endpoints/" + id);

        // GET never returns the secret again.
        JsonNode fetched = json(get("/v1/endpoints/" + id));
        assertThat(fetched.has("secret")).isFalse();
        assertThat(fetched.get("eventTypes").get(0).asString()).isEqualTo(type);

        // At rest it is AES-GCM ciphertext that decrypts back to exactly what was shown.
        var row = jdbc.sql("""
                        SELECT s.id, s.key_id, s.ciphertext FROM endpoints e JOIN endpoint_secrets s ON s.id = e.secret_ref
                         WHERE e.id = ?""")
                .param(id)
                .query((rs, n) -> new Object[] {rs.getObject(1, UUID.class), rs.getString(2), rs.getBytes(3)})
                .single();
        byte[] ciphertext = (byte[]) row[2];
        assertThat(new String(ciphertext, StandardCharsets.ISO_8859_1)).doesNotContain(secret);
        assertThat(cipher.decrypt((UUID) row[0], (String) row[1], ciphertext)).isEqualTo(secret);
    }

    @Test
    void validatesInput() {
        assertThat(post("/v1/endpoints", "{\"url\":\"ftp://x/\",\"eventTypes\":[\"order.created\"]}"))
                .hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(post("/v1/endpoints", "{\"url\":\"http://x/\",\"eventTypes\":[]}"))
                .hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(post("/v1/endpoints", "{\"url\":\"http://x/\",\"eventTypes\":[\"Not A Type\"]}"))
                .hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(post("/v1/endpoints", "{\"url\":\"http://x/\",\"eventTypes\":[\"order.created\"],\"rateLimitPerSec\":0}"))
                .hasStatus(HttpStatus.BAD_REQUEST);
        assertThat(get("/v1/endpoints/" + UUID.randomUUID())).hasStatus(HttpStatus.NOT_FOUND);
    }

    @Test
    void requiresBearerToken() {
        assertThat(mvc.get().uri("/v1/endpoints").exchange()).hasStatus(HttpStatus.UNAUTHORIZED);
        assertThat(mvc.get().uri("/v1/endpoints").header("Authorization", "Bearer wrong").exchange())
                .hasStatus(HttpStatus.UNAUTHORIZED);
        assertThat(mvc.get().uri("/actuator/health").exchange()).hasStatus(HttpStatus.OK);
    }

    @Test
    void patchUpdatesFieldsAndDeleteHidesEndpoint() {
        String type = uniqueType("user");
        UUID id = createEndpoint(List.of(type));

        MvcTestResult patched = patch("/v1/endpoints/" + id, """
                {"rateLimitPerSec":7,"eventTypes":["%s","*"],"status":"PAUSED"}""".formatted(type));
        assertThat(patched).hasStatus(HttpStatus.OK);
        JsonNode body = json(patched);
        assertThat(body.get("rateLimitPerSec").asInt()).isEqualTo(7);
        assertThat(body.get("status").asString()).isEqualTo("PAUSED");
        assertThat(body.get("eventTypes")).hasSize(2);

        assertThat(patch("/v1/endpoints/" + id, "{\"status\":\"DELETED\"}")).hasStatus(HttpStatus.BAD_REQUEST);

        assertThat(delete("/v1/endpoints/" + id)).hasStatus(HttpStatus.NO_CONTENT);
        assertThat(get("/v1/endpoints/" + id)).hasStatus(HttpStatus.NOT_FOUND);
        assertThat(delete("/v1/endpoints/" + id)).hasStatus(HttpStatus.NOT_FOUND);
    }

    @Test
    void resumingAPausedEndpointReenqueuesParkedDeliveries() {
        String type = uniqueType("invoice");
        UUID id = createEndpoint(List.of(type));
        assertThat(post("/v1/events", "{\"type\":\"%s\",\"data\":{\"n\":1}}".formatted(type)))
                .hasStatus(HttpStatus.ACCEPTED);
        patch("/v1/endpoints/" + id, "{\"status\":\"PAUSED\"}");
        // Simulate the dispatcher parking the delivery while the endpoint is paused.
        jdbc.sql("UPDATE deliveries SET status = 'PARKED' WHERE endpoint_id = ?").param(id).update();
        long outboxBefore = jdbc.sql("SELECT count(*) FROM outbox WHERE endpoint_id = ?").param(id).query(Long.class).single();

        assertThat(patch("/v1/endpoints/" + id, "{\"status\":\"ACTIVE\"}")).hasStatus(HttpStatus.OK);

        assertThat(jdbc.sql("SELECT status FROM deliveries WHERE endpoint_id = ?").param(id).query(String.class).single())
                .isEqualTo("PENDING");
        assertThat(jdbc.sql("SELECT count(*) FROM outbox WHERE endpoint_id = ?").param(id).query(Long.class).single())
                .isEqualTo(outboxBefore + 1);
    }
}
