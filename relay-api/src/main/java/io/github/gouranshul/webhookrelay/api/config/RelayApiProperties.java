package io.github.gouranshul.webhookrelay.api.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("relay")
public record RelayApiProperties(
        @DefaultValue Crypto crypto,
        @DefaultValue Api api,
        @DefaultValue Events events,
        @DefaultValue Endpoints endpoints,
        @DefaultValue Outbox outbox,
        @DefaultValue Kafka kafka,
        @DefaultValue Retention retention) {

    /** How long finished deliveries (and their attempts) and events are kept. */
    public record Retention(@DefaultValue("30d") Duration deliveries) {
    }

    /** AES-256 key (base64) that encrypts endpoint signing secrets at rest. */
    public record Crypto(String key, @DefaultValue("k1") String keyId) {
    }

    /** Static bearer token for the management API. Blank disables authentication (local development only). */
    public record Api(String token) {
    }

    public record Events(
            @DefaultValue("262144") int maxPayloadBytes,
            @DefaultValue("24h") Duration idempotencyTtl) {
    }

    public record Endpoints(
            @DefaultValue("false") boolean allowPrivateNetworks,
            @DefaultValue("50") int defaultRateLimitPerSec) {
    }

    public record Outbox(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("2") int workers,
            @DefaultValue("200") int batchSize,
            @DefaultValue("100ms") Duration pollInterval,
            @DefaultValue("10s") Duration publishTimeout,
            @DefaultValue("1h") Duration retention) {
    }

    public record Kafka(@DefaultValue("48") int partitions, @DefaultValue("1") short replicationFactor) {
    }
}
