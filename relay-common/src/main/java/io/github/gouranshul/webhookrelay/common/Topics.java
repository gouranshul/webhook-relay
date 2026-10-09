package io.github.gouranshul.webhookrelay.common;

/** Kafka topic names shared by the API (producer) and the dispatcher (consumer). */
public final class Topics {

    /** Main delivery topic. Records are keyed by endpoint id so first attempts stay ordered per endpoint. */
    public static final String DELIVERIES = "webhook.deliveries";

    /** Operational events emitted by the relay itself, e.g. an endpoint being auto-paused. */
    public static final String SYSTEM_EVENTS = "webhook.system-events";

    private Topics() {
    }
}
