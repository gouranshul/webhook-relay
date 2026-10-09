package io.github.gouranshul.webhookrelay.common.testing;

import java.security.SecureRandom;
import java.util.Base64;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Singleton Testcontainers shared by every test class in a JVM. Started lazily on first use and never
 * stopped explicitly (Ryuk removes them when the JVM exits). Image tags are pinned here, and only here.
 *
 * <p>Because containers are shared, tests must not depend on a clean database: each test creates its own
 * endpoints and uses its own event types.
 */
public final class Containers {

    public static final String POSTGRES_IMAGE = "postgres:18.6-alpine";
    public static final String KAFKA_IMAGE = "apache/kafka:4.3.1";
    public static final String REDIS_IMAGE = "redis:8.10.2-alpine";
    public static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.12.0";

    private static PostgreSQLContainer postgres;
    private static KafkaContainer kafka;
    private static GenericContainer<?> redis;

    private Containers() {
    }

    public static synchronized PostgreSQLContainer postgres() {
        if (postgres == null) {
            postgres = new PostgreSQLContainer(POSTGRES_IMAGE)
                    .withDatabaseName("relay")
                    .withCommand("postgres", "-c", "max_connections=300", "-c", "fsync=off");
            postgres.start();
        }
        return postgres;
    }

    public static synchronized KafkaContainer kafka() {
        if (kafka == null) {
            kafka = new KafkaContainer(KAFKA_IMAGE)
                    .withEnv("KAFKA_NUM_PARTITIONS", "12")
                    .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
            kafka.start();
        }
        return kafka;
    }

    public static synchronized GenericContainer<?> redis() {
        if (redis == null) {
            redis = new GenericContainer<>(REDIS_IMAGE).withExposedPorts(6379);
            redis.start();
        }
        return redis;
    }

    /** A fresh AES-256 key per JVM, so no key material is ever committed, not even for tests. */
    public static final String SECRET_KEY = randomKeyBase64();

    private static String randomKeyBase64() {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        return Base64.getEncoder().encodeToString(key);
    }
}
