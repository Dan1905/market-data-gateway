package com.mdg.gateway.client;

import com.mdg.gateway.config.ExchangeProperties;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Exponential backoff with bounded jitter.
 *
 * <p>The jitter is not decoration. Every gateway instance watching the same venue sees the
 * same disconnect at the same moment; without randomization they all reconnect in lockstep
 * and re-create the load spike that caused the outage, then synchronize harder on each
 * subsequent attempt. Spreading attempts over a window breaks that cycle.
 *
 * <p>With the default {@code jitter = 0.5}, attempt <i>n</i> waits a uniformly random
 * duration in {@code [0.5 × d, d]} where {@code d = min(initial × multiplier^(n-1), max)} —
 * the "equal jitter" strategy, which keeps a useful lower bound on the delay rather than
 * occasionally retrying instantly.
 *
 * <p>The random source is injectable so tests can assert the exact bounds instead of
 * sampling and hoping.
 */
public final class BackoffPolicy {

    private final long initialDelayMillis;
    private final long maxDelayMillis;
    private final double multiplier;
    private final double jitter;
    private final DoubleSupplier randomSource;

    public BackoffPolicy(ExchangeProperties.Backoff config) {
        this(config, () -> ThreadLocalRandom.current().nextDouble());
    }

    BackoffPolicy(ExchangeProperties.Backoff config, DoubleSupplier randomSource) {
        this.initialDelayMillis = Math.max(1, config.initialDelay().toMillis());
        this.maxDelayMillis = Math.max(initialDelayMillis, config.maxDelay().toMillis());
        this.multiplier = config.multiplier() < 1.0 ? 1.0 : config.multiplier();
        this.jitter = Math.clamp(config.jitter(), 0.0, 1.0);
        this.randomSource = randomSource;
    }

    /**
     * @param attempt 1-based consecutive failure count
     * @return how long to wait before the next connect attempt
     */
    public Duration delayForAttempt(int attempt) {
        int normalized = Math.max(1, attempt);

        // Math.pow can overflow to Infinity for large attempt counts; min() absorbs that
        // safely, which is why the cap is applied in double space before the cast.
        double exponential = initialDelayMillis * Math.pow(multiplier, normalized - 1.0);
        long capped = (long) Math.min(exponential, (double) maxDelayMillis);

        double jitterWindow = capped * jitter;
        long delay = (long) (capped - jitterWindow + randomSource.getAsDouble() * jitterWindow);
        return Duration.ofMillis(Math.max(0, delay));
    }
}
