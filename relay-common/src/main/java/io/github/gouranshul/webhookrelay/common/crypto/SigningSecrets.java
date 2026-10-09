package io.github.gouranshul.webhookrelay.common.crypto;

import java.security.SecureRandom;
import java.util.Base64;

/** Generates endpoint signing secrets: {@code whsec_} followed by 32 random bytes, base64url-encoded. */
public final class SigningSecrets {

    public static final String PREFIX = "whsec_";
    private static final SecureRandom RANDOM = new SecureRandom();

    private SigningSecrets() {
    }

    public static String generate() {
        byte[] bytes = new byte[32];
        RANDOM.nextBytes(bytes);
        return PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
