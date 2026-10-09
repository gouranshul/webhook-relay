package io.github.gouranshul.webhookrelay.receiver;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Default behaviour for hooks without an override. Set from env in docker compose. */
@ConfigurationProperties("receiver")
public record ReceiverProperties(
        @DefaultValue("0") double failureRate,
        @DefaultValue("0") long minLatencyMs,
        @DefaultValue("0") long maxLatencyMs,
        @DefaultValue("503") int failureStatus,
        @DefaultValue("10000") int maxRecordedPerHook,
        @DefaultValue("10000") int maxHooks) {

    Behavior defaultBehavior() {
        return new Behavior(failureRate, minLatencyMs, maxLatencyMs, failureStatus, null, null, null);
    }
}
