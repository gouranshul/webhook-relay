package io.github.gouranshul.webhookrelay.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

/** The mock receiver shares this classpath; each must read only its own config file. */
class ConfigLoadingTest extends DispatcherIntegrationTest {

    @Autowired
    Environment environment;

    @Test
    void dispatcherAndEmbeddedReceiverReadTheirOwnConfig() {
        assertThat(environment.getProperty("spring.application.name")).isEqualTo("relay-dispatcher");
        assertThat(environment.getProperty("spring.kafka.consumer.group-id")).isEqualTo("relay-dispatcher");
        assertThat(RECEIVER.state().defaults().failureStatus()).isEqualTo(503);
    }
}
