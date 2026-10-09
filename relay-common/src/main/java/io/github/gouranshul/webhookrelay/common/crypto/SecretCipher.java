package io.github.gouranshul.webhookrelay.common.crypto;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Encrypts endpoint signing secrets at rest with AES-256-GCM.
 *
 * <p>Output layout is {@code iv(12) || ciphertext || tag(16)}. The secret's row id is bound as additional
 * authenticated data, so a ciphertext copied onto another row fails to decrypt. Thread-safe.
 */
public final class SecretCipher {

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int IV_BYTES = 12;
    private static final int TAG_BITS = 128;

    private final String keyId;
    private final SecretKeySpec key;
    private final SecureRandom random = new SecureRandom();

    public SecretCipher(String keyId, byte[] keyBytes) {
        this.keyId = Objects.requireNonNull(keyId, "keyId");
        if (keyBytes == null || keyBytes.length != 32) {
            throw new IllegalArgumentException("Secret encryption key must be exactly 32 bytes (AES-256)");
        }
        this.key = new SecretKeySpec(keyBytes.clone(), "AES");
    }

    /** Builds a cipher from a base64 key, failing fast with a readable message when misconfigured. */
    public static SecretCipher fromBase64(String keyId, String base64Key) {
        if (base64Key == null || base64Key.isBlank()) {
            throw new IllegalStateException("RELAY_SECRET_KEY is not set (base64 of 32 random bytes)");
        }
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(base64Key.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("RELAY_SECRET_KEY is not valid base64", e);
        }
        return new SecretCipher(keyId, bytes);
    }

    public String keyId() {
        return keyId;
    }

    public byte[] encrypt(UUID secretId, String plaintext) {
        byte[] iv = new byte[IV_BYTES];
        random.nextBytes(iv);
        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            cipher.updateAAD(aad(secretId));
            byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.allocate(IV_BYTES + ciphertext.length).put(iv).put(ciphertext).array();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Secret encryption failed", e);
        }
    }

    public String decrypt(UUID secretId, String storedKeyId, byte[] stored) {
        if (!keyId.equals(storedKeyId)) {
            throw new IllegalStateException("Secret was encrypted with an unknown key id: " + storedKeyId);
        }
        if (stored == null || stored.length <= IV_BYTES) {
            throw new IllegalStateException("Stored secret is truncated");
        }
        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, stored, 0, IV_BYTES));
            cipher.updateAAD(aad(secretId));
            byte[] plain = cipher.doFinal(stored, IV_BYTES, stored.length - IV_BYTES);
            return new String(plain, StandardCharsets.UTF_8);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Secret decryption failed (wrong key or tampered ciphertext)", e);
        }
    }

    private static byte[] aad(UUID secretId) {
        return ByteBuffer.allocate(16)
                .putLong(secretId.getMostSignificantBits())
                .putLong(secretId.getLeastSignificantBits())
                .array();
    }
}
