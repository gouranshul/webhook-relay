package io.github.gouranshul.webhookrelay.api.config;

import io.github.gouranshul.webhookrelay.common.Topics;
import io.github.gouranshul.webhookrelay.common.crypto.SecretCipher;
import io.github.gouranshul.webhookrelay.common.net.EndpointUrlPolicy;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration(proxyBeanMethods = false)
public class ApiConfig {

    @Bean
    SecretCipher secretCipher(RelayApiProperties properties) {
        return SecretCipher.fromBase64(properties.crypto().keyId(), properties.crypto().key());
    }

    @Bean
    EndpointUrlPolicy endpointUrlPolicy(RelayApiProperties properties) {
        return new EndpointUrlPolicy(properties.endpoints().allowPrivateNetworks());
    }

    @Bean
    NewTopic deliveriesTopic(RelayApiProperties properties) {
        return TopicBuilder.name(Topics.DELIVERIES)
                .partitions(properties.kafka().partitions())
                .replicas(properties.kafka().replicationFactor())
                .build();
    }
}
