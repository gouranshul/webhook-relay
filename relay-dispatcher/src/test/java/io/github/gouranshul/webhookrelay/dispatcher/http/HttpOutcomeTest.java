package io.github.gouranshul.webhookrelay.dispatcher.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.gouranshul.webhookrelay.dispatcher.http.HttpOutcome.Kind;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

class HttpOutcomeTest {

    @ParameterizedTest
    @CsvSource({
        "200, SUCCESS", "201, SUCCESS", "204, SUCCESS", "299, SUCCESS",
        "500, RETRYABLE", "502, RETRYABLE", "503, RETRYABLE", "504, RETRYABLE", "408, RETRYABLE", "429, RETRYABLE",
        "400, PERMANENT", "401, PERMANENT", "403, PERMANENT", "404, PERMANENT", "410, PERMANENT", "422, PERMANENT",
        "301, PERMANENT", "302, PERMANENT", "100, PERMANENT"
    })
    void classifiesStatusCodes(int status, Kind expected) {
        assertThat(HttpOutcome.classify(status)).isEqualTo(expected);
    }

    @Test
    void onlyNetworkAndServerFailuresAreRetryable() {
        assertThat(Kind.RETRYABLE.retryable()).isTrue();
        assertThat(Kind.TIMEOUT.retryable()).isTrue();
        assertThat(Kind.CONNECTION_ERROR.retryable()).isTrue();
        assertThat(Kind.PERMANENT.retryable()).isFalse();
        assertThat(Kind.SUCCESS.retryable()).isFalse();
    }

    @Test
    void parsesRetryAfterSecondsAndHttpDates() {
        Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        assertThat(RetryAfter.parse("120", clock)).isEqualTo(Duration.ofMinutes(2));
        assertThat(RetryAfter.parse(" 5 ", clock)).isEqualTo(Duration.ofSeconds(5));
        assertThat(RetryAfter.parse("Thu, 01 Jan 2026 00:00:30 GMT", clock)).isEqualTo(Duration.ofSeconds(30));
        assertThat(RetryAfter.parse("Wed, 31 Dec 2025 23:59:00 GMT", clock)).isNull();
        assertThat(RetryAfter.parse("0", clock)).isNull();
        assertThat(RetryAfter.parse("-3", clock)).isNull();
        assertThat(RetryAfter.parse("soon", clock)).isNull();
        assertThat(RetryAfter.parse(null, clock)).isNull();
    }
}
