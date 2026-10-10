package com.mdg.gateway.client;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/** Parsing of the HTTP {@code Retry-After} header. */
public final class RetryAfter {

    private RetryAfter() {
    }

    /**
     * {@code Retry-After} is either delay-seconds or an HTTP-date (RFC 9110).
     *
     * @return the delay, never negative; {@code null} if the value is neither form
     */
    public static Duration parse(String value) {
        String trimmed = value.trim();
        try {
            return Duration.ofSeconds(Math.max(0, Long.parseLong(trimmed)));
        } catch (NumberFormatException notSeconds) {
            try {
                Instant when = ZonedDateTime.parse(trimmed, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
                Duration until = Duration.between(Instant.now(), when);
                return until.isNegative() ? Duration.ZERO : until;
            } catch (DateTimeParseException unparseable) {
                return null;
            }
        }
    }
}
