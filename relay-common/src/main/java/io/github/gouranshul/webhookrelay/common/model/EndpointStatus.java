package io.github.gouranshul.webhookrelay.common.model;

public enum EndpointStatus {
    ACTIVE,
    /** Paused by an operator or automatically after too many consecutive DLT entries. Deliveries are parked. */
    PAUSED,
    /** Soft-deleted. Outstanding deliveries are cancelled. */
    DELETED
}
