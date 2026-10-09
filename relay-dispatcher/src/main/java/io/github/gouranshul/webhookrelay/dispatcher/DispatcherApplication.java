package io.github.gouranshul.webhookrelay.dispatcher;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class DispatcherApplication {

    /** Config file name; unique per app so several apps can share one classpath (end-to-end tests). */
    public static final String CONFIG_NAME = "spring.config.name=relay-dispatcher";

    public static void main(String[] args) {
        new SpringApplicationBuilder(DispatcherApplication.class).properties(CONFIG_NAME).run(args);
    }
}
