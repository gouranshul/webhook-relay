package io.github.gouranshul.webhookrelay.dispatcher.delivery;

/**
 * Retrying cannot help: a 4xx other than 408/429, a redirect, a URL that violates the SSRF policy, or an
 * unreadable message. Routes the record straight to the DLT.
 */
public class PermanentDeliveryException extends RuntimeException {

    public PermanentDeliveryException(String message) {
        super(message, null, false, false);
    }

    public PermanentDeliveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
