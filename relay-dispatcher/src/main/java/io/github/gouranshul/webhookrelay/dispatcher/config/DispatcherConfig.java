package io.github.gouranshul.webhookrelay.dispatcher.config;

import io.github.gouranshul.webhookrelay.common.Topics;
import io.github.gouranshul.webhookrelay.common.crypto.SecretCipher;
import io.github.gouranshul.webhookrelay.common.net.EndpointUrlPolicy;
import io.github.gouranshul.webhookrelay.dispatcher.http.WebhookSender;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration(proxyBeanMethods = false)
public class DispatcherConfig {

    private static final Logger log = LoggerFactory.getLogger(DispatcherConfig.class);

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    SecretCipher secretCipher(DispatcherProperties properties) {
        return SecretCipher.fromBase64(properties.crypto().keyId(), properties.crypto().key());
    }

    @Bean
    EndpointUrlPolicy endpointUrlPolicy(DispatcherProperties properties) {
        return new EndpointUrlPolicy(properties.endpoints().allowPrivateNetworks());
    }

    /**
     * Runs webhook HTTP attempts. "virtual": a virtual thread per attempt, so a slow receiver costs a few KB of
     * heap instead of a 1 MB platform stack. "platform": a fixed pool, kept for the benchmark comparison.
     */
    @Bean(destroyMethod = "close")
    ExecutorService deliveryExecutor(DispatcherProperties properties) {
        DispatcherProperties.Dispatcher config = properties.dispatcher();
        if ("platform".equalsIgnoreCase(config.executor())) {
            log.info("Delivery executor: fixed pool of {} platform threads", config.platformPoolSize());
            return Executors.newFixedThreadPool(config.platformPoolSize(),
                    Thread.ofPlatform().name("delivery-", 0).factory());
        }
        log.info("Delivery executor: virtual threads");
        return Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("delivery-", 0).factory());
    }

    @Bean
    WebhookSender webhookSender(DispatcherProperties properties, EndpointUrlPolicy policy,
            ExecutorService deliveryExecutor, Clock clock) {
        return new WebhookSender(properties.dispatcher(), policy, deliveryExecutor, clock);
    }

    @Bean
    NewTopic deliveriesTopic(DispatcherProperties properties) {
        return TopicBuilder.name(Topics.DELIVERIES)
                .partitions(properties.kafka().partitions())
                .replicas(properties.kafka().replicationFactor())
                .build();
    }

    @Bean
    NewTopic systemEventsTopic(DispatcherProperties properties) {
        return TopicBuilder.name(Topics.SYSTEM_EVENTS)
                .partitions(1)
                .replicas(properties.kafka().replicationFactor())
                .build();
    }
}
