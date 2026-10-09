package io.github.gouranshul.webhookrelay.common.model;

import java.util.Set;

/** Lifecycle of a delivery row. Terminal states never transition again (a replay creates a new row). */
public enum DeliveryStatus {
    /** Written by the API together with its outbox row; not yet attempted. */
    PENDING,
    /** A dispatcher holds a lease and an HTTP attempt is running. */
    IN_FLIGHT,
    /** At least one attempt failed with a retryable outcome; the next attempt is scheduled on a retry topic. */
    RETRYING,
    /** The receiver answered 2xx. Terminal. */
    SUCCEEDED,
    /** Permanent failure or retries exhausted; the record went to the DLT. Terminal. */
    FAILED,
    /** The endpoint is paused; the delivery waits until the endpoint is resumed. */
    PARKED,
    /** The endpoint was deleted before the delivery completed. Terminal. */
    CANCELLED;

    public static final Set<DeliveryStatus> TERMINAL = Set.of(SUCCEEDED, FAILED, CANCELLED);

    public boolean isTerminal() {
        return TERMINAL.contains(this);
    }
}
