package com.mdg.gateway.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Root of the gateway's own configuration surface, bound from {@code gateway.*}.
 *
 * <p>Records with {@code @DefaultValue} rather than mutable JavaBeans: the config is read
 * from many virtual threads concurrently, and constructor binding means an invalid
 * deployment fails at startup instead of at the first tick.
 */
@Validated
@ConfigurationProperties(prefix = "gateway")
public record GatewayProperties(

        // @DefaultValue on each nested block lets a context start with no `gateway.*` at
        // all — which is what the test slices rely on — while still validating whatever is
        // supplied. Without it, an absent block binds to null and trips @NotNull.
        @Valid @NotNull @DefaultValue Topics topics,

        @Valid @NotNull @DefaultValue Symbol symbol,

        @Valid @NotNull @DefaultValue Producer producer,

        @Valid @NotNull @DefaultValue Dlq dlq) {

    public record Topics(

            @DefaultValue("normalized-market-data") @NotBlank String normalized,

            @DefaultValue("market-data-dlq") @NotBlank String deadLetter,

            /**
             * Single partition keeps a t3.micro Redpanda honest; raise for real throughput.
             * Ordering is per-partition, so partition count is also an ordering decision.
             */
            @DefaultValue("3") @Positive int partitions,

            @DefaultValue("1") @Positive short replicationFactor,

            /**
             * How long normalized events are kept.
             *
             * <p>Sized for a small instance, not for storage. Measured on a live run this
             * pipeline writes ~1 GB/day at ~25 events/sec across three venues, so the
             * original 7-day window needed ~7.6 GB — more than an EC2 free-tier root
             * volume. Market data is replayable from the venue itself, so this is an audit
             * window; raise it only if you have attached the disk to match.
             */
            @DefaultValue("24h") @NotNull Duration normalizedRetention,

            /** Longer than the primary topic so there is time to fix a bug and replay. */
            @DefaultValue("7d") @NotNull Duration deadLetterRetention) {
    }

    public record Symbol(

            /**
             * Map USDT/USDC/BUSD-quoted pairs onto {@code -USD}. See
             * {@link com.mdg.gateway.mapper.SymbolNormalizer} for why this is a real
             * modelling decision and not just cosmetics.
             */
            @DefaultValue("true") boolean collapseStablecoins) {
    }

    public record Producer(

            /**
             * How long a virtual thread will park on the broker ack before the send is
             * declared failed and handed to the circuit breaker. Blocking here is cheap
             * precisely because the carrier thread is released.
             */
            @DefaultValue("5s") @NotNull Duration sendTimeout) {
    }

    public record Dlq(

            /** Consumer group prefix for replay runs; a run suffixes it with a UUID. */
            @DefaultValue("market-data-gateway-replay") @NotBlank String replayGroupPrefix,

            /** Hard ceiling on a single replay run, regardless of what the caller asks for. */
            @DefaultValue("5m") @NotNull Duration replayMaxDuration,

            /** How many distinct failure messages to echo back in the replay response. */
            @DefaultValue("10") @Positive int failureSampleLimit) {
    }
}
