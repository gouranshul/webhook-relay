package io.github.gouranshul.webhookrelay.receiver;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Starts the mock receiver inside another JVM's tests (dispatcher and end-to-end tests), on a random port.
 * Auto-configurations that the host's classpath brings along (database, Kafka, Redis, tracing export) are
 * switched off, so the receiver stays a plain web app.
 */
public final class EmbeddedReceiver implements AutoCloseable {

    private static final String EXCLUDES = String.join(",",
            "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
            "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration",
            "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration",
            "org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration",
            "org.springframework.boot.data.redis.autoconfigure.DataRedisRepositoriesAutoConfiguration");

    private final ConfigurableApplicationContext context;

    private EmbeddedReceiver(ConfigurableApplicationContext context) {
        this.context = context;
    }

    public static EmbeddedReceiver start(String... properties) {
        // Passed as command-line arguments so they win over mock-receiver.yml (e.g. its fixed port 8090).
        List<String> args = new ArrayList<>(List.of(
                "--server.port=0",
                "--spring.jmx.enabled=false",
                "--spring.autoconfigure.exclude=" + EXCLUDES,
                "--management.tracing.export.enabled=false"));
        for (String property : properties) {
            args.add("--" + property);
        }
        ConfigurableApplicationContext context = new SpringApplicationBuilder(MockReceiverApplication.class)
                .properties(MockReceiverApplication.CONFIG_NAME)
                .run(args.toArray(String[]::new));
        return new EmbeddedReceiver(context);
    }

    public int port() {
        return Integer.parseInt(context.getEnvironment().getRequiredProperty("local.server.port"));
    }

    public String url(String hook) {
        return "http://127.0.0.1:" + port() + "/hooks/" + hook;
    }

    public ReceiverState state() {
        return context.getBean(ReceiverState.class);
    }

    @Override
    public void close() {
        context.close();
    }
}
