package io.github.gouranshul.webhookrelay.dispatcher.http;

import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/** Parses {@code Retry-After}: either delay-seconds or an HTTP-date (RFC 9110 section 10.2.3). */
public final class RetryAfter {

    private RetryAfter() {
    }

    /** Returns the delay, or null if the header is absent, malformed or in the past. */
    public static Duration parse(String value, Clock clock) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String trimmed = value.trim();
        try {
            long seconds = Long.parseLong(trimmed);
            return seconds > 0 ? Duration.ofSeconds(seconds) : null;
        } catch (NumberFormatException notSeconds) {
            try {
                ZonedDateTime when = ZonedDateTime.parse(trimmed, DateTimeFormatter.RFC_1123_DATE_TIME);
                Duration delay = Duration.between(clock.instant(), when.toInstant());
                return delay.isNegative() || delay.isZero() ? null : delay;
            } catch (DateTimeParseException notADate) {
                return null;
            }
        }
    }
}
