package com.mdg.gateway.client;

import com.mdg.gateway.config.ExchangeProperties;
import com.mdg.gateway.model.Exchange;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;

class VenueConnectionGuardTest {

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final CircuitBreaker breaker = CircuitBreaker.of("venue-test", CircuitBreakerConfig.custom()
            .slidingWindowSize(2).minimumNumberOfCalls(2).failureRateThreshold(100)
            .waitDurationInOpenState(Duration.ofMinutes(1)).build());
    private final VenueConnectionGuard guard = new VenueConnectionGuard(
            Exchange.BINANCE,
            new ExchangeProperties.Limits(5, Duration.ofSeconds(30), Duration.ofSeconds(90), Duration.ofMinutes(15)),
            breaker, meterRegistry);

    @Test
    @DisplayName("Retry-After parses as seconds or as an HTTP date; garbage is ignored")
    void parsesRetryAfter() {
        assertThat(VenueConnectionGuard.parseRetryAfter("120")).isEqualTo(Duration.ofSeconds(120));
        assertThat(VenueConnectionGuard.parseRetryAfter(" 7 ")).isEqualTo(Duration.ofSeconds(7));
        assertThat(VenueConnectionGuard.parseRetryAfter("-5")).isEqualTo(Duration.ZERO);

        String inOneMinute = DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(60));
        assertThat(VenueConnectionGuard.parseRetryAfter(inOneMinute))
                .isBetween(Duration.ofSeconds(50), Duration.ofSeconds(61));
        String inThePast = DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC).minusHours(1));
        assertThat(VenueConnectionGuard.parseRetryAfter(inThePast)).isEqualTo(Duration.ZERO);

        assertThat(VenueConnectionGuard.parseRetryAfter("soon")).isNull();
    }

    @Test
    @DisplayName("only policy-violation and try-again-later close codes mean 'rate limited'")
    void classifiesCloseCodes() {
        assertThat(VenueConnectionGuard.isRateLimitClose(1008)).isTrue();
        assertThat(VenueConnectionGuard.isRateLimitClose(1013)).isTrue();
        assertThat(VenueConnectionGuard.isRateLimitClose(1000)).isFalse();
        assertThat(VenueConnectionGuard.isRateLimitClose(1006)).isFalse();
        assertThat(VenueConnectionGuard.isRateLimitClose(1011)).isFalse();
    }

    @Test
    @DisplayName("an ordinary connect failure carries no rate-limit signal")
    void ordinaryFailureIsNotARateLimit() {
        assertThat(VenueConnectionGuard.rateLimitSignal(
                new CompletionException(new java.net.ConnectException("refused")))).isEmpty();
    }

    @Test
    @DisplayName("a rate-limit signal starts the default cooldown and opens the breaker")
    void rateLimitedWithoutRetryAfter() {
        assertThat(guard.cooldownRemaining()).isEqualTo(Duration.ZERO);

        guard.rateLimited(null, "close code 1008");

        assertThat(guard.cooldownRemaining()).isBetween(Duration.ofSeconds(85), Duration.ofSeconds(90));
        assertThat(guard.circuitState()).isEqualTo("OPEN");
        assertThat(guard.tryBegin()).isEmpty();
        assertThat(meterRegistry.counter("mdg.ws.rate.limited", "exchange", "BINANCE").count()).isEqualTo(1.0);
        assertThat(meterRegistry.counter("mdg.ws.connect.blocked", "exchange", "BINANCE").count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Retry-After is honoured up to the cap, and a second signal never shortens the cooldown")
    void retryAfterBoundsAndMonotonicity() {
        guard.rateLimited(Duration.ofHours(6), "HTTP 418");
        assertThat(guard.cooldownRemaining()).isBetween(Duration.ofMinutes(14), Duration.ofMinutes(15));

        // Already OPEN: must not throw on the redundant transition, and must not shorten.
        guard.rateLimited(Duration.ofSeconds(1), "HTTP 429");
        assertThat(guard.cooldownRemaining()).isGreaterThan(Duration.ofMinutes(14));
    }

    @Test
    @DisplayName("an attempt reports exactly one outcome, whichever callback gets there first")
    void attemptSettlesOnce() {
        VenueConnectionGuard.Attempt attempt = guard.tryBegin().orElseThrow();

        assertThat(attempt.stable()).isTrue();
        attempt.failed(new IllegalStateException("late onClose"));   // ignored
        assertThat(attempt.stable()).isFalse();

        assertThat(breaker.getMetrics().getNumberOfSuccessfulCalls()).isEqualTo(1);
        assertThat(breaker.getMetrics().getNumberOfFailedCalls()).isZero();
    }

    @Test
    @DisplayName("consecutive failed attempts open the breaker; an abandoned one counts as neither")
    void failuresOpenBreaker() {
        guard.tryBegin().orElseThrow().abandoned();   // our own shutdown
        assertThat(breaker.getMetrics().getNumberOfBufferedCalls()).isZero();

        guard.tryBegin().orElseThrow().failed(new IllegalStateException("closed by peer: 1006"));
        assertThat(guard.circuitState()).isEqualTo("CLOSED");
        guard.tryBegin().orElseThrow().failed(new IllegalStateException("closed by peer: 1006"));

        assertThat(guard.circuitState()).isEqualTo("OPEN");
        assertThat(guard.tryBegin()).isEmpty();
    }
}
