package io.github.gouranshul.webhookrelay.e2e;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.github.gouranshul.webhookrelay.api.RelayApiApplication;
import io.github.gouranshul.webhookrelay.common.testing.Containers;
import io.github.gouranshul.webhookrelay.dispatcher.DispatcherApplication;
import io.github.gouranshul.webhookrelay.receiver.EmbeddedReceiver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

/**
 * The whole system in one JVM: relay-api, relay-dispatcher and the mock receiver as Spring Boot apps, against
 * real Postgres (behind Toxiproxy, so latency can be injected), Kafka (on a fixed host port, so the container
 * can be restarted and clients reconnect to the same address) and Redis.
 */
final class RelayStack implements AutoCloseable {

    static final String API_TOKEN = "e2e-" + UUID.randomUUID();

    private final Network network = Network.newNetwork();
    final PostgreSQLContainer postgres = new PostgreSQLContainer(Containers.POSTGRES_IMAGE)
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withDatabaseName("relay")
            .withCommand("postgres", "-c", "max_connections=200");
    final ToxiproxyContainer toxiproxy = new ToxiproxyContainer(Containers.TOXIPROXY_IMAGE).withNetwork(network);
    final KafkaContainer kafka;
    final GenericContainer<?> redis = new GenericContainer<>(Containers.REDIS_IMAGE).withExposedPorts(6379);

    Proxy postgresProxy;
    EmbeddedReceiver receiver;
    ConfigurableApplicationContext api;
    ConfigurableApplicationContext dispatcher;
    JdbcClient jdbc;

    RelayStack() {
        int kafkaPort = freePort();
        kafka = new KafkaContainer(Containers.KAFKA_IMAGE)
                .withEnv("KAFKA_NUM_PARTITIONS", "6")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
        kafka.setPortBindings(List.of(kafkaPort + ":9092"));
    }

    RelayStack start(String... dispatcherOverrides) throws IOException {
        postgres.start();
        toxiproxy.start();
        kafka.start();
        redis.start();

        ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        postgresProxy = client.createProxy("postgres", "0.0.0.0:8666", "postgres:5432");
        String jdbcUrl = "jdbc:postgresql://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(8666) + "/relay";

        receiver = EmbeddedReceiver.start();

        List<String> common = List.of(
                "--server.port=0",
                "--spring.datasource.url=" + jdbcUrl,
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--spring.datasource.hikari.connection-timeout=30000",
                "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                "--relay.crypto.key=" + Containers.SECRET_KEY,
                "--relay.endpoints.allow-private-networks=true",
                "--relay.kafka.partitions=6",
                "--relay.kafka.retry-partitions=6",
                "--relay.dispatcher.concurrency=6",
                "--management.tracing.sampling.probability=0.0");

        List<String> apiArgs = new ArrayList<>(common);
        apiArgs.add("--relay.api.token=" + API_TOKEN);
        api = new SpringApplicationBuilder(RelayApiApplication.class)
                .properties(RelayApiApplication.CONFIG_NAME)
                .run(apiArgs.toArray(String[]::new));
        jdbc = api.getBean(JdbcClient.class);

        List<String> dispatcherArgs = new ArrayList<>(common);
        dispatcherArgs.addAll(List.of(
                "--spring.data.redis.host=" + redis.getHost(),
                "--spring.data.redis.port=" + redis.getMappedPort(6379),
                "--relay.endpoints.cache-ttl=1s",
                "--relay.dispatcher.request-timeout=5s",
                "--relay.retry.attempts=10",
                "--relay.retry.initial-delay=1s",
                "--relay.retry.multiplier=2",
                "--relay.retry.max-delay=30s",
                "--relay.auto-pause.consecutive-dlt=0",
                "--relay.dispatcher.lease-sweep-interval=5s"));
        for (String override : dispatcherOverrides) {
            dispatcherArgs.add("--" + override);
        }
        dispatcher = new SpringApplicationBuilder(DispatcherApplication.class)
                .properties(DispatcherApplication.CONFIG_NAME)
                .run(dispatcherArgs.toArray(String[]::new));
        return this;
    }

    String apiUrl() {
        return "http://127.0.0.1:" + api.getEnvironment().getRequiredProperty("local.server.port");
    }

    /** Restarts the Kafka broker in place (same container, same data, same host port). */
    void restartKafka() {
        DockerClientFactory.instance().client().restartContainerCmd(kafka.getContainerId()).withTimeout(10).exec();
    }

    /** Kills the broker (SIGKILL, no graceful shutdown), waits {@code downtime}, then starts the same container. */
    void killKafkaFor(java.time.Duration downtime) throws InterruptedException {
        var docker = DockerClientFactory.instance().client();
        docker.killContainerCmd(kafka.getContainerId()).exec();
        Thread.sleep(downtime);
        docker.startContainerCmd(kafka.getContainerId()).exec();
    }

    /** Counter value from the dispatcher's meter registry. */
    double dispatcherCounter(String name) {
        var counter = dispatcher.getBean(io.micrometer.core.instrument.MeterRegistry.class).find(name).counter();
        return counter == null ? 0 : counter.count();
    }

    @Override
    public void close() {
        for (AutoCloseable c : new AutoCloseable[] {dispatcher, api, receiver}) {
            try {
                if (c != null) {
                    c.close();
                }
            } catch (Exception ignored) {
                // best effort
            }
        }
        redis.stop();
        kafka.stop();
        toxiproxy.stop();
        postgres.stop();
        network.close();
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
