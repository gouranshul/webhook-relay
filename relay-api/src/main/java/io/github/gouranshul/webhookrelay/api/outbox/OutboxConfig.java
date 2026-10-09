package io.github.gouranshul.webhookrelay.api.outbox;

import io.github.gouranshul.webhookrelay.api.config.RelayApiProperties;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

@Configuration(proxyBeanMethods = false)
public class OutboxConfig {

    @Bean
    @ConditionalOnBooleanProperty(name = "relay.outbox.enabled", matchIfMissing = true)
    OutboxRelay outboxRelay(OutboxRepository repository, KafkaTemplate<String, String> kafka, ObjectMapper mapper,
            PlatformTransactionManager txManager, RelayApiProperties properties, MeterRegistry registry) {
        return new OutboxRelay("outbox-relay@" + hostname(), repository, kafka, mapper,
                new TransactionTemplate(txManager), properties.outbox(), registry);
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }
}
