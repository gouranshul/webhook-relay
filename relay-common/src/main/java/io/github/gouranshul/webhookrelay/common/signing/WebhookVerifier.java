package io.github.gouranshul.webhookrelay.common.signing;

import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.util.HexFormat;

/**
 * Verifies webhook signatures on the receiving side.
 *
 * <p>Rejects timestamps further than the tolerance (default 5 minutes) from the receiver's clock in either
 * direction, which bounds the window in which a captured request can be replayed. Signatures are compared in
 * constant time.
 */
public final class WebhookVerifier {

    public static final Duration DEFAULT_TOLERANCE = Duration.ofMinutes(5);
    private static final HexFormat HEX = HexFormat.of();

    public enum Result {
        VALID,
        MISSING_HEADERS,
        MALFORMED_TIMESTAMP,
        TIMESTAMP_TOO_OLD,
        TIMESTAMP_IN_FUTURE,
        INVALID_SIGNATURE;

        public boolean isValid() {
            return this == VALID;
        }
    }

    private final Clock clock;
    private final Duration tolerance;

    public WebhookVerifier() {
        this(Clock.systemUTC(), DEFAULT_TOLERANCE);
    }

    public WebhookVerifier(Clock clock, Duration tolerance) {
        this.clock = clock;
        this.tolerance = tolerance;
    }

    /**
     * @param secret          the endpoint's signing secret
     * @param timestampHeader value of {@code Webhook-Timestamp}
     * @param signatureHeader value of {@code Webhook-Signature}, possibly several space-separated {@code v1=} entries
     * @param body            the raw request body exactly as received
     */
    public Result verify(String secret, String timestampHeader, String signatureHeader, byte[] body) {
        if (secret == null || timestampHeader == null || signatureHeader == null || body == null) {
            return Result.MISSING_HEADERS;
        }
        long timestamp;
        try {
            timestamp = Long.parseLong(timestampHeader.trim());
        } catch (NumberFormatException e) {
            return Result.MALFORMED_TIMESTAMP;
        }
        long now = clock.instant().getEpochSecond();
        if (timestamp < now - tolerance.toSeconds()) {
            return Result.TIMESTAMP_TOO_OLD;
        }
        if (timestamp > now + tolerance.toSeconds()) {
            return Result.TIMESTAMP_IN_FUTURE;
        }
        byte[] expected = WebhookSigner.hmac(secret, timestamp, body);
        for (String candidate : signatureHeader.trim().split("[\\s,]+")) {
            if (!candidate.startsWith(WebhookSigner.VERSION + "=")) {
                continue;
            }
            byte[] provided;
            try {
                provided = HEX.parseHex(candidate.substring(WebhookSigner.VERSION.length() + 1));
            } catch (IllegalArgumentException e) {
                continue;
            }
            if (MessageDigest.isEqual(expected, provided)) {
                return Result.VALID;
            }
        }
        return Result.INVALID_SIGNATURE;
    }
}
