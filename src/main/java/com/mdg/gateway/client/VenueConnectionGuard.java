package com.mdg.gateway.client;

import com.mdg.gateway.config.ExchangeProperties;
import com.mdg.gateway.model.Exchange;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpHeaders;
import java.net.http.WebSocketHandshakeException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Decides whether this gateway is currently allowed to dial a venue.
 *
 * <h2>Why backoff alone is not enough</h2>
 * Exponential backoff slows down retries of a connection that <em>fails to open</em>. It does
 * nothing for a venue that accepts the handshake and then hangs up — the usual shape of a
 * rate limit or an IP-level throttle — because every accepted handshake looks like success
 * and resets the backoff. The result is one reconnect a second, forever, which is exactly
 * the behaviour Binance documents as a reason to ban an IP.
 *
 * <p>Two mechanisms here close that gap:
 * <ul>
 *   <li><b>A circuit breaker per venue.</b> A connection attempt only counts as a success
 *       once the socket has stayed up for {@code stableAfter}. Enough consecutive failures —
 *       refused handshakes or short-lived sockets alike — open the breaker, and while it is
 *       open no connection attempt is made at all. After its wait it allows a single probe.</li>
 *   <li><b>A cooldown when the venue says so.</b> An HTTP 429/418 handshake response or a
 *       policy-violation close code is an explicit "slow down". The venue's
 *       {@code Retry-After} is honoured (bounded by {@code maxRetryAfter}); without one,
 *       {@code rateLimitCooldown} applies. The breaker is opened as well, so the state is
 *       visible in metrics and health.</li>
 * </ul>
 */
final class VenueConnectionGuard {

    private static final Logger log = LoggerFactory.getLogger(VenueConnectionGuard.class);

    /** 429 Too Many Requests; 418 is what Binance returns once an IP has been auto-banned. */
    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    private static final int HTTP_BANNED = 418;

    /** 1008 Policy Violation and 1013 Try Again Later: the WebSocket-level "slow down". */
    private static final int CLOSE_POLICY_VIOLATION = 1008;
    private static final int CLOSE_TRY_AGAIN_LATER = 1013;

    private final Exchange exchange;
    private final ExchangeProperties.Limits limits;
    private final CircuitBreaker breaker;
    private final Counter rateLimitedCounter;
    private final Counter blockedCounter;
    private final AtomicReference<Instant> notBefore = new AtomicReference<>(Instant.MIN);

    VenueConnectionGuard(Exchange exchange,
                         ExchangeProperties.Limits limits,
                         CircuitBreaker breaker,
                         MeterRegistry meterRegistry) {
        this.exchange = exchange;
        this.limits = limits;
        this.breaker = breaker;
        this.rateLimitedCounter = Counter.builder("mdg.ws.rate.limited")
                .tag("exchange", exchange.name())
                .description("Times the venue told us to slow down (HTTP 429/418 or a policy close code)")
                .register(meterRegistry);
        this.blockedCounter = Counter.builder("mdg.ws.connect.blocked")
                .tag("exchange", exchange.name())
                .description("Connection attempts not made because the venue circuit breaker was open")
                .register(meterRegistry);
    }

    /** How long the venue has asked us to stay away; zero when no cooldown is active. */
    Duration cooldownRemaining() {
        Duration remaining = Duration.between(Instant.now(), notBefore.get());
        return remaining.isNegative() ? Duration.ZERO : remaining;
    }

    /**
     * Asks permission for one connection attempt.
     *
     * @return the attempt to report the outcome on, or empty when the breaker refuses
     */
    Optional<Attempt> tryBegin() {
        if (!breaker.tryAcquirePermission()) {
            blockedCounter.increment();
            return Optional.empty();
        }
        return Optional.of(new Attempt());
    }

    String circuitState() {
        return breaker.getState().name();
    }

    /** Records an explicit slow-down signal from the venue. */
    void rateLimited(Duration retryAfter, String signal) {
        Duration cooldown = retryAfter != null ? retryAfter : limits.rateLimitCooldown();
        if (cooldown.compareTo(limits.maxRetryAfter()) > 0) {
            // A hostile or broken Retry-After must not park a venue for a day.
            cooldown = limits.maxRetryAfter();
        }
        Instant until = Instant.now().plus(cooldown);
        notBefore.accumulateAndGet(until, (current, candidate) -> candidate.isAfter(current) ? candidate : current);
        rateLimitedCounter.increment();
        log.warn("{} rate limited ({}); not reconnecting for {} s", exchange, signal, cooldown.toSeconds());

        if (breaker.getState() == CircuitBreaker.State.CLOSED
                || breaker.getState() == CircuitBreaker.State.HALF_OPEN) {
            breaker.transitionToOpenState();
        }
    }

    /** The slow-down signal carried by a failed handshake, if there is one. */
    static Optional<RateLimitSignal> rateLimitSignal(Throwable connectError) {
        for (Throwable t = connectError; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof WebSocketHandshakeException handshake && handshake.getResponse() != null) {
                int status = handshake.getResponse().statusCode();
                if (status == HTTP_TOO_MANY_REQUESTS || status == HTTP_BANNED) {
                    return Optional.of(new RateLimitSignal(status, retryAfter(handshake.getResponse().headers())));
                }
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    static boolean isRateLimitClose(int closeCode) {
        return closeCode == CLOSE_POLICY_VIOLATION || closeCode == CLOSE_TRY_AGAIN_LATER;
    }

    private static Duration retryAfter(HttpHeaders headers) {
        return headers.firstValue("Retry-After").map(VenueConnectionGuard::parseRetryAfter).orElse(null);
    }

    /** {@code Retry-After} is either delay-seconds or an HTTP-date (RFC 9110). */
    static Duration parseRetryAfter(String value) {
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

    record RateLimitSignal(int httpStatus, Duration retryAfter) {
    }

    /**
     * One permitted connection attempt. Exactly one outcome is recorded on the breaker, however
     * many of the socket's callbacks race to report it.
     */
    final class Attempt {

        private final long startNanos = System.nanoTime();
        private final AtomicBoolean settled = new AtomicBoolean();

        /** The socket stayed up long enough to count as a working connection. */
        boolean stable() {
            if (!settled.compareAndSet(false, true)) {
                return false;
            }
            breaker.onSuccess(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
            return true;
        }

        /** The handshake failed, or the socket was lost before it became stable. */
        void failed(Throwable cause) {
            if (settled.compareAndSet(false, true)) {
                breaker.onError(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS, cause);
            }
        }

        /** We closed it ourselves (shutdown): neither a success nor a failure. */
        void abandoned() {
            if (settled.compareAndSet(false, true)) {
                breaker.releasePermission();
            }
        }
    }
}
