package io.github.gouranshul.webhookrelay.common.crypto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SecretCipherTest {

    private static byte[] randomKey() {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        return key;
    }

    private final SecretCipher cipher = new SecretCipher("k1", randomKey());

    @Test
    void roundTrips() {
        UUID id = UUID.randomUUID();
        String secret = SigningSecrets.generate();
        byte[] stored = cipher.encrypt(id, secret);
        assertThat(new String(stored, java.nio.charset.StandardCharsets.ISO_8859_1)).doesNotContain(secret);
        assertThat(cipher.decrypt(id, "k1", stored)).isEqualTo(secret);
    }

    @Test
    void usesFreshIvPerEncryption() {
        UUID id = UUID.randomUUID();
        assertThat(cipher.encrypt(id, "same")).isNotEqualTo(cipher.encrypt(id, "same"));
    }

    @Test
    void ciphertextIsBoundToItsRowId() {
        byte[] stored = cipher.encrypt(UUID.randomUUID(), "whsec_abc");
        assertThatThrownBy(() -> cipher.decrypt(UUID.randomUUID(), "k1", stored))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("decryption failed");
    }

    @Test
    void detectsTampering() {
        UUID id = UUID.randomUUID();
        byte[] stored = cipher.encrypt(id, "whsec_abc");
        stored[stored.length - 1] ^= 1;
        assertThatThrownBy(() -> cipher.decrypt(id, "k1", stored)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsWrongKeyAndUnknownKeyId() {
        UUID id = UUID.randomUUID();
        byte[] stored = cipher.encrypt(id, "whsec_abc");
        assertThatThrownBy(() -> new SecretCipher("k1", randomKey()).decrypt(id, "k1", stored))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> cipher.decrypt(id, "k0", stored)).hasMessageContaining("unknown key id");
        assertThatThrownBy(() -> cipher.decrypt(id, "k1", new byte[5])).hasMessageContaining("truncated");
    }

    @Test
    void validatesKeyMaterial() {
        assertThatThrownBy(() -> new SecretCipher("k1", new byte[16])).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SecretCipher.fromBase64("k1", "")).hasMessageContaining("not set");
        assertThatThrownBy(() -> SecretCipher.fromBase64("k1", "%%%")).hasMessageContaining("not valid base64");
        String b64 = Base64.getEncoder().encodeToString(randomKey());
        assertThat(SecretCipher.fromBase64("k2", b64).keyId()).isEqualTo("k2");
    }

    @Test
    void generatesPrefixedHighEntropySecrets() {
        String a = SigningSecrets.generate();
        assertThat(a).startsWith("whsec_").hasSize("whsec_".length() + 43);
        assertThat(a).isNotEqualTo(SigningSecrets.generate());
    }
}
