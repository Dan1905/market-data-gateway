package com.mdg.gateway.config;

import com.mdg.gateway.model.Exchange;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Per-venue connection settings, bound from {@code gateway.exchanges.<venue>.*}.
 *
 * <p>Keyed by the {@link Exchange} enum so a typo in YAML is a startup binding failure,
 * and so adding a fourth venue is a config entry plus one client subclass.
 */
@Validated
@ConfigurationProperties(prefix = "gateway")
public record ExchangeProperties(@Valid Map<Exchange, Connection> exchanges) {

    /**
     * Normalizes the bound map to an {@link EnumMap} and tolerates an entirely absent
     * {@code gateway.exchanges} block — which is exactly what the test profile relies on
     * to start the context with no venue clients at all.
     */
    public ExchangeProperties {
        exchanges = exchanges == null || exchanges.isEmpty()
                ? new EnumMap<>(Exchange.class)
                : new EnumMap<>(exchanges);
    }

    public Connection get(Exchange exchange) {
        Connection connection = exchanges.get(exchange);
        if (connection == null) {
            throw new IllegalStateException(
                    "No configuration bound for gateway.exchanges." + exchange.name().toLowerCase());
        }
        return connection;
    }

    public record Connection(

            /**
             * Master switch. Disabled venues are never constructed, which is how the test
             * profile guarantees CI makes no outbound calls to public exchanges.
             */
            @DefaultValue("true") boolean enabled,

            @NotNull URI uri,

            /** Venue-native instrument codes, e.g. {@code BTCUSDT} / {@code BTC-USD} / {@code BTC/USD}. */
            @NotEmpty List<String> symbols,

            @Valid @NotNull @DefaultValue Backoff backoff,

            /** TCP/TLS + handshake budget for a single connect attempt. */
            @DefaultValue("10s") @NotNull Duration connectTimeout,

            /**
             * Reconnect if no frame arrives within this window. Venue-level heartbeats are
             * handled by the JDK client's automatic Pong, but a socket can stay open and
             * silent after a venue-side fault — this is the only thing that catches that.
             */
            @DefaultValue("45s") @NotNull Duration idleTimeout,

            /** Guard against a hostile or buggy peer streaming an unbounded fragmented frame. */
            @DefaultValue("1048576") int maxFrameBytes,

            @Valid @NotNull @DefaultValue Limits limits,

            /**
             * Base URL of the venue's REST API, used to recover trades missed while the socket
             * was down. Leave unset to switch gap recovery off for this venue.
             */
            URI restUrl) {
    }

    /**
     * How this gateway stays inside a venue's published limits (see {@code VenueConnectionGuard}).
     */
    public record Limits(

            /**
             * Ceiling on frames we send per second on one connection. Binance allows 5 and
             * counts pongs and every JSON control message against it, so it is configured
             * lower than its limit rather than at it.
             */
            @DefaultValue("5") @Positive int outboundMessagesPerSecond,

            /**
             * How long a socket must stay up before the connection counts as a success. A
             * venue that accepts the handshake and hangs up straight away is failing, not
             * succeeding, and must not reset the backoff or close the circuit breaker.
             */
            @DefaultValue("30s") @NotNull Duration stableAfter,

            /** Cooldown after a rate-limit signal that carried no {@code Retry-After}. */
            @DefaultValue("2m") @NotNull Duration rateLimitCooldown,

            /** Upper bound on a venue-supplied {@code Retry-After}. */
            @DefaultValue("15m") @NotNull Duration maxRetryAfter) {
    }

    /** Exponential backoff with full jitter. */
    public record Backoff(

            @DefaultValue("1s") @NotNull Duration initialDelay,

            @DefaultValue("60s") @NotNull Duration maxDelay,

            @DefaultValue("2.0") double multiplier,

            /**
             * Fraction of the computed delay randomized away, in {@code [0,1]}. Without
             * jitter every client reconnects in lockstep after a venue outage and
             * re-creates the thundering herd that caused it.
             */
            @DefaultValue("0.5") double jitter) {
    }
}
