package io.github.gouranshul.webhookrelay.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

/** Each app reads its own config file (relay-api.yml), so apps can share a classpath in end-to-end tests. */
class ConfigLoadingTest extends ApiIntegrationTest {

    @Autowired
    Environment environment;

    @Test
    void loadsRelayApiYml() {
        assertThat(environment.getProperty("spring.application.name")).isEqualTo("relay-api");
        assertThat(environment.getProperty("spring.kafka.producer.acks")).isEqualTo("all");
    }
}
