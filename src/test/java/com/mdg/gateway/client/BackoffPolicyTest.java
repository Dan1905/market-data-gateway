package com.mdg.gateway.client;

import com.mdg.gateway.config.ExchangeProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffPolicyTest {

    private static final ExchangeProperties.Backoff DEFAULTS = new ExchangeProperties.Backoff(
            Duration.ofSeconds(1), Duration.ofSeconds(60), 2.0, 0.5);

    /** Jitter pinned to its maximum so the exponential schedule itself is observable. */
    private static BackoffPolicy noJitter(ExchangeProperties.Backoff config) {
        return new BackoffPolicy(config, () -> 1.0);
    }

    @ParameterizedTest(name = "attempt {0} -> {1} ms")
    @CsvSource({
            "1, 1000",
            "2, 2000",
            "3, 4000",
            "4, 8000",
            "5, 16000",
            "6, 32000",
            "7, 60000",   // capped
            "20, 60000"   // still capped, no overflow
    })
    void followsAnExponentialScheduleAndHonoursTheCap(int attempt, long expectedMillis) {
        assertThat(noJitter(DEFAULTS).delayForAttempt(attempt).toMillis()).isEqualTo(expectedMillis);
    }

    @Test
    @DisplayName("Math.pow overflow at absurd attempt counts still yields the cap")
    void doesNotOverflowOnHugeAttemptCounts() {
        assertThat(noJitter(DEFAULTS).delayForAttempt(Integer.MAX_VALUE))
                .isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    @DisplayName("jitter spreads attempts over [0.5d, d] so instances do not reconnect in lockstep")
    void appliesEqualJitterWindow() {
        // Lower bound of the window...
        BackoffPolicy lowest = new BackoffPolicy(DEFAULTS, () -> 0.0);
        // ...and the upper bound.
        BackoffPolicy highest = new BackoffPolicy(DEFAULTS, () -> 1.0);

        assertThat(lowest.delayForAttempt(3).toMillis()).isEqualTo(2000);  // 0.5 x 4000
        assertThat(highest.delayForAttempt(3).toMillis()).isEqualTo(4000);
    }

    @Test
    void zeroJitterProducesADeterministicDelay() {
        BackoffPolicy policy = new BackoffPolicy(
                new ExchangeProperties.Backoff(Duration.ofSeconds(1), Duration.ofSeconds(60), 2.0, 0.0),
                () -> 0.0);

        assertThat(policy.delayForAttempt(3).toMillis()).isEqualTo(4000);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
    void treatsNonPositiveAttemptsAsTheFirstAttempt(int attempt) {
        assertThat(noJitter(DEFAULTS).delayForAttempt(attempt).toMillis()).isEqualTo(1000);
    }

    @Test
    @DisplayName("nonsensical configuration is clamped rather than producing negative delays")
    void clampsInvalidConfiguration() {
        // A multiplier below 1 would shrink delays on every failure — the opposite of backoff.
        BackoffPolicy shrinking = noJitter(new ExchangeProperties.Backoff(
                Duration.ofSeconds(2), Duration.ofSeconds(60), 0.1, 0.0));
        assertThat(shrinking.delayForAttempt(5).toMillis()).isEqualTo(2000);

        // Jitter outside [0,1] would make the delay negative or overshoot the cap.
        BackoffPolicy overJittered = new BackoffPolicy(
                new ExchangeProperties.Backoff(Duration.ofSeconds(2), Duration.ofSeconds(60), 2.0, 5.0),
                () -> 0.0);
        assertThat(overJittered.delayForAttempt(1)).isGreaterThanOrEqualTo(Duration.ZERO);

        // maxDelay below initialDelay must not invert the bounds.
        BackoffPolicy inverted = noJitter(new ExchangeProperties.Backoff(
                Duration.ofSeconds(10), Duration.ofSeconds(1), 2.0, 0.0));
        assertThat(inverted.delayForAttempt(1).toMillis()).isEqualTo(10_000);
    }
}
