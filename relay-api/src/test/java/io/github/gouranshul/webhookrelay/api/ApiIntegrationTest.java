package io.github.gouranshul.webhookrelay.api;

import io.github.gouranshul.webhookrelay.common.testing.Containers;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Real Postgres and Kafka. The outbox relay bean is disabled by default so tests that count Kafka records
 * control exactly which relays run.
 */
@SpringBootTest(properties = "relay.outbox.enabled=false")
@AutoConfigureMockMvc
public abstract class ApiIntegrationTest {

    protected static final String TOKEN = "test-token-" + UUID.randomUUID();

    @ServiceConnection
    protected static final PostgreSQLContainer POSTGRES = Containers.postgres();

    @ServiceConnection
    protected static final KafkaContainer KAFKA = Containers.kafka();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("relay.crypto.key", () -> Containers.SECRET_KEY);
        registry.add("relay.api.token", () -> TOKEN);
        registry.add("relay.endpoints.allow-private-networks", () -> "true");
    }

    @Autowired
    protected MockMvcTester mvc;

    @Autowired
    protected ObjectMapper mapper;

    @Autowired
    protected JdbcClient jdbc;

    /** An event type no other test uses, so shared-container state never leaks between tests. */
    protected static String uniqueType(String prefix) {
        return prefix + ".t" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    protected MvcTestResult post(String uri, String json, String... headers) {
        var request = mvc.post().uri(uri).contentType(MediaType.APPLICATION_JSON).content(json)
                .header("Authorization", "Bearer " + TOKEN);
        for (int i = 0; i < headers.length; i += 2) {
            request = request.header(headers[i], headers[i + 1]);
        }
        return request.exchange();
    }

    protected MvcTestResult get(String uri) {
        return mvc.get().uri(uri).header("Authorization", "Bearer " + TOKEN).exchange();
    }

    protected MvcTestResult patch(String uri, String json) {
        return mvc.patch().uri(uri).contentType(MediaType.APPLICATION_JSON).content(json)
                .header("Authorization", "Bearer " + TOKEN).exchange();
    }

    protected MvcTestResult delete(String uri) {
        return mvc.delete().uri(uri).header("Authorization", "Bearer " + TOKEN).exchange();
    }

    protected JsonNode json(MvcTestResult result) {
        try {
            return mapper.readTree(result.getResponse().getContentAsString());
        } catch (java.io.UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Registers an endpoint subscribed to the given types and returns its id. */
    protected UUID createEndpoint(List<String> types) {
        String body = mapper.writeValueAsString(new java.util.LinkedHashMap<>(java.util.Map.of(
                "url", "http://localhost:9/hooks/" + UUID.randomUUID(),
                "eventTypes", types)));
        MvcTestResult result = post("/v1/endpoints", body);
        if (result.getResponse().getStatus() != 201) {
            throw new IllegalStateException("endpoint creation failed: " + result.getResponse().getStatus());
        }
        return UUID.fromString(json(result).get("id").asString());
    }
}
