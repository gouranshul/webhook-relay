package io.github.gouranshul.webhookrelay.common.signing;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gouranshul.webhookrelay.common.signing.WebhookVerifier.Result;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class WebhookSignatureTest {

    private static final String SECRET = "whsec_test_secret";
    private static final long TS = 1_700_000_000L;
    private static final byte[] BODY = "{\"id\":\"evt_1\",\"type\":\"order.created\"}".getBytes(StandardCharsets.UTF_8);

    private final WebhookSigner signer = new WebhookSigner();
    private final WebhookVerifier verifier =
            new WebhookVerifier(Clock.fixed(Instant.ofEpochSecond(TS + 10), ZoneOffset.UTC), Duration.ofMinutes(5));

    @Test
    void signatureMatchesIndependentOpensslVector() {
        // printf '%s' '1700000000.{"id":"evt_1","type":"order.created"}' | openssl dgst -sha256 -hmac 'whsec_test_secret'
        assertThat(signer.sign(SECRET, TS, BODY))
                .isEqualTo("v1=479d263a26872ce2aeda9f217b43b4c9870314fba787aeb383c38d7c7a296e24");
    }

    @Test
    void acceptsOwnSignature() {
        String header = signer.sign(SECRET, TS, BODY);
        assertThat(verifier.verify(SECRET, Long.toString(TS), header, BODY)).isEqualTo(Result.VALID);
    }

    @Test
    void rejectsTamperedBody() {
        String header = signer.sign(SECRET, TS, BODY);
        byte[] tampered = "{\"id\":\"evt_1\",\"type\":\"order.deleted\"}".getBytes(StandardCharsets.UTF_8);
        assertThat(verifier.verify(SECRET, Long.toString(TS), header, tampered)).isEqualTo(Result.INVALID_SIGNATURE);
    }

    @Test
    void rejectsWrongSecret() {
        String header = signer.sign("whsec_other", TS, BODY);
        assertThat(verifier.verify(SECRET, Long.toString(TS), header, BODY)).isEqualTo(Result.INVALID_SIGNATURE);
    }

    @Test
    void rejectsTimestampSwappedAfterSigning() {
        String header = signer.sign(SECRET, TS, BODY);
        assertThat(verifier.verify(SECRET, Long.toString(TS + 1), header, BODY)).isEqualTo(Result.INVALID_SIGNATURE);
    }

    @Test
    void rejectsTimestampsOutsideTolerance() {
        long old = TS + 10 - 301;
        long future = TS + 10 + 301;
        assertThat(verifier.verify(SECRET, Long.toString(old), signer.sign(SECRET, old, BODY), BODY))
                .isEqualTo(Result.TIMESTAMP_TOO_OLD);
        assertThat(verifier.verify(SECRET, Long.toString(future), signer.sign(SECRET, future, BODY), BODY))
                .isEqualTo(Result.TIMESTAMP_IN_FUTURE);
    }

    @Test
    void acceptsTimestampAtToleranceBoundary() {
        long edge = TS + 10 - 300;
        assertThat(verifier.verify(SECRET, Long.toString(edge), signer.sign(SECRET, edge, BODY), BODY))
                .isEqualTo(Result.VALID);
    }

    @Test
    void acceptsAnyMatchingSignatureDuringRotation() {
        String header = signer.sign("whsec_old", TS, BODY) + " " + signer.sign(SECRET, TS, BODY);
        assertThat(verifier.verify(SECRET, Long.toString(TS), header, BODY)).isEqualTo(Result.VALID);
    }

    @Test
    void handlesMalformedInput() {
        assertThat(verifier.verify(SECRET, null, "v1=00", BODY)).isEqualTo(Result.MISSING_HEADERS);
        assertThat(verifier.verify(SECRET, "yesterday", "v1=00", BODY)).isEqualTo(Result.MALFORMED_TIMESTAMP);
        assertThat(verifier.verify(SECRET, Long.toString(TS), "v1=not-hex v2=abc garbage", BODY))
                .isEqualTo(Result.INVALID_SIGNATURE);
        assertThat(Result.VALID.isValid()).isTrue();
        assertThat(Result.INVALID_SIGNATURE.isValid()).isFalse();
    }
}
