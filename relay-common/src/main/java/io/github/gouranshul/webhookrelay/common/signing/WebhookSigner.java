package io.github.gouranshul.webhookrelay.common.signing;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Computes {@code v1} signatures: {@code hex(HMAC-SHA256(secret, timestamp + "." + body))}.
 *
 * <p>Stateless and thread-safe: a fresh {@link Mac} is created per call because {@code Mac} is not thread-safe.
 */
public final class WebhookSigner {

    public static final String VERSION = "v1";
    private static final String ALGORITHM = "HmacSHA256";
    private static final HexFormat HEX = HexFormat.of();

    /** Returns the header value, e.g. {@code v1=5257a869...}. */
    public String sign(String secret, long timestampSeconds, byte[] body) {
        return VERSION + "=" + HEX.formatHex(hmac(secret, timestampSeconds, body));
    }

    static byte[] hmac(String secret, long timestampSeconds, byte[] body) {
        Objects.requireNonNull(secret, "secret");
        Objects.requireNonNull(body, "body");
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM));
            mac.update(Long.toString(timestampSeconds).getBytes(StandardCharsets.US_ASCII));
            mac.update((byte) '.');
            return mac.doFinal(body);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("HMAC-SHA256 unavailable", e);
        }
    }
}
