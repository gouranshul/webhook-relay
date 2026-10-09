package io.github.gouranshul.webhookrelay.dispatcher.endpoint;

import io.github.gouranshul.webhookrelay.common.model.EndpointStatus;
import java.util.UUID;

/**
 * What the dispatcher needs to deliver to an endpoint. Holds the decrypted secret, so it lives only in the
 * bounded, short-lived endpoint cache and is never logged ({@link #toString()} redacts it).
 */
public record EndpointConfig(UUID id, String url, String secret, int rateLimitPerSec, EndpointStatus status) {

    /** Stand-in for an endpoint that no longer exists. */
    public static EndpointConfig missing(UUID id) {
        return new EndpointConfig(id, null, null, 1, EndpointStatus.DELETED);
    }

    @Override
    public String toString() {
        return "EndpointConfig[id=" + id + ", url=" + url + ", rateLimitPerSec=" + rateLimitPerSec
                + ", status=" + status + ", secret=<redacted>]";
    }
}
