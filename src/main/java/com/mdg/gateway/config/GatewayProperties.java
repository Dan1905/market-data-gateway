package com.mdg.gateway.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.nio.file.Path;
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

        @Valid @NotNull @DefaultValue Dlq dlq,

        @Valid @NotNull @DefaultValue Dedup dedup,

        @Valid @NotNull @DefaultValue Lanes lanes,

        @Valid @NotNull @DefaultValue Spill spill,

        @Valid @NotNull @DefaultValue Backfill backfill) {

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

    /**
     * In-gateway deduplication of trades by their deterministic event id.
     *
     * <p>Duplicates come from reconnect overlap, venue snapshots sent on (re)subscribe, REST
     * backfill overlapping the live stream, and DLQ replays of events that did reach the broker
     * but whose acknowledgement was lost. All of them are close in time to the original, so a
     * bounded time window catches them without unbounded memory. Beyond the window, the
     * deterministic {@code eventId} still lets any consumer deduplicate.
     */
    public record Dedup(

            @DefaultValue("true") boolean enabled,

            /**
             * Hard cap on remembered ids. ~150 bytes each, so 100k is roughly 15 MB — bounded
             * even if the window would otherwise hold more at high message rates.
             */
            @DefaultValue("100000") @Positive int maxEntries,

            /** How long an id is remembered. Must exceed the longest expected reconnect gap. */
            @DefaultValue("10m") @NotNull Duration window) {
    }

    /**
     * Per-symbol ordered lanes (see {@code SymbolLaneDispatcher}).
     */
    public record Lanes(

            /**
             * Global budget of events queued or publishing across all lanes. When exhausted, the
             * socket reader blocks, which stops reading frames and pushes TCP backpressure back to
             * the venue. Each in-flight event is ~1 KB, so 1000 bounds this at about 1 MB.
             */
            @DefaultValue("1000") @Positive int maxInFlight,

            /** On shutdown, how long to let queued events finish publishing. */
            @DefaultValue("10s") @NotNull Duration drainTimeout) {
    }

    /**
     * Local disk buffer for events the broker would not take (see {@code SpillBuffer}).
     */
    public record Spill(

            @DefaultValue("true") boolean enabled,

            /** Must be on a volume that survives a container restart, or a redeploy loses it. */
            @DefaultValue("data/spill") @NotNull Path directory,

            /**
             * Hard cap on disk used. At ~600 bytes an event, 256 MB holds roughly 400k events:
             * about five hours of a three-venue BTC feed. Past it, events go to the DLQ instead.
             */
            @DefaultValue("268435456") @Positive long maxBytes,

            @DefaultValue("4194304") @Positive long segmentBytes,

            /**
             * fsync every append. Costs a few milliseconds per event, and only while the broker
             * is down; turning it off trades that for losing the last moments on a host crash.
             */
            @DefaultValue("true") boolean fsync,

            /** Records sent to the broker per drain round. */
            @DefaultValue("200") @Positive int drainBatchSize,

            /** Pause between drain attempts while the broker is still refusing. */
            @DefaultValue("1s") @NotNull Duration retryInterval) {
    }

    /**
     * Recovery of trades missed while a socket was down or the gateway was restarting (see
     * {@code TradeSequencer} and {@code GapBackfillService}).
     */
    public record Backfill(

            @DefaultValue("true") boolean enabled,

            /**
             * Last delivered trade id per venue symbol. Must be on a volume: it is what lets a
             * restart or redeploy notice, and recover, the trades it missed while down.
             */
            @DefaultValue("data/state/trade-positions.json") @NotNull Path stateFile,

            /**
             * Most trades recovered for one gap. A longer gap (a host that was off for a day)
             * recovers only its newest part; the rest is counted as missing. Bounds both the
             * REST calls made and how long one symbol's lane is held.
             */
            @DefaultValue("20000") @Positive int maxGap,

            @DefaultValue("5s") @NotNull Duration requestTimeout,

            /** REST calls per second, per venue. Far below every venue's public limit. */
            @DefaultValue("2") @Positive int callsPerSecond) {
    }
}
