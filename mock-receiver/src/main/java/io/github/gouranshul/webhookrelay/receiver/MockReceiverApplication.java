package io.github.gouranshul.webhookrelay.receiver;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class MockReceiverApplication {

    /** Config file name; unique per app so several apps can share one classpath (end-to-end tests). */
    public static final String CONFIG_NAME = "spring.config.name=mock-receiver";

    public static void main(String[] args) {
        new SpringApplicationBuilder(MockReceiverApplication.class).properties(CONFIG_NAME).run(args);
    }
}
