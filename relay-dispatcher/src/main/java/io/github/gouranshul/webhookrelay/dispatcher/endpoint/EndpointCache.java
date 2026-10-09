package io.github.gouranshul.webhookrelay.dispatcher.endpoint;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.github.gouranshul.webhookrelay.common.crypto.SecretCipher;
import io.github.gouranshul.webhookrelay.common.model.EndpointStatus;
import io.github.gouranshul.webhookrelay.dispatcher.config.DispatcherProperties;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * Endpoint lookups for every message would hit Postgres thousands of times a second, so configs are cached.
 * Bounded by size (LRU-ish W-TinyLFU) and by TTL: an operator's PATCH takes effect within the TTL.
 */
@Component
public class EndpointCache {

    private final LoadingCache<UUID, EndpointConfig> cache;
    private final JdbcClient jdbc;
    private final SecretCipher cipher;

    public EndpointCache(JdbcClient jdbc, SecretCipher cipher, DispatcherProperties properties) {
        this.jdbc = jdbc;
        this.cipher = cipher;
        this.cache = Caffeine.newBuilder()
                .maximumSize(properties.endpoints().cacheMaxSize())
                .expireAfterWrite(properties.endpoints().cacheTtl())
                .build(this::load);
    }

    public EndpointConfig get(UUID endpointId) {
        return cache.get(endpointId);
    }

    public void invalidate(UUID endpointId) {
        cache.invalidate(endpointId);
    }

    private EndpointConfig load(UUID id) {
        return jdbc.sql("""
                        SELECT e.url, e.rate_limit_per_sec, e.status, s.id AS secret_id, s.key_id, s.ciphertext
                          FROM endpoints e JOIN endpoint_secrets s ON s.id = e.secret_ref
                         WHERE e.id = ?""")
                .param(id)
                .query((rs, n) -> new EndpointConfig(
                        id,
                        rs.getString("url"),
                        cipher.decrypt(rs.getObject("secret_id", UUID.class), rs.getString("key_id"),
                                rs.getBytes("ciphertext")),
                        rs.getInt("rate_limit_per_sec"),
                        EndpointStatus.valueOf(rs.getString("status"))))
                .optional()
                .orElseGet(() -> EndpointConfig.missing(id));
    }
}
