package com.mdg.gateway.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.mdg.gateway.config.GatewayProperties;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Map;

/**
 * Drops trades the gateway has already delivered, keyed by their deterministic event id.
 *
 * <h2>Claim, then release on failure</h2>
 * A trade is {@link #claim claimed} <em>before</em> it is published, atomically
 * ({@code putIfAbsent}), so two copies racing through the pipeline cannot both get through. If
 * delivery then fails, the claim is {@link #release released} — otherwise a later copy of a
 * trade that never actually reached the broker would be discarded as a duplicate of nothing,
 * which is data loss dressed up as deduplication.
 *
 * <h2>Why a bounded window is enough</h2>
 * Every source of duplicates is close in time to the original: reconnect overlap, the snapshot
 * a venue replays on subscribe, a REST backfill overlapping the live stream, a DLQ replay of an
 * event whose broker ack was lost. A size- and time-bounded cache catches all of them with a
 * fixed memory ceiling. Past the window, the deterministic {@code eventId} still lets any
 * consumer deduplicate — this is an optimisation of the stream, not the only line of defence.
 *
 * <p>Delivery semantics remain at-least-once end to end. This removes the duplicates the
 * gateway can see; it does not claim exactly-once.
 */
@Component
public class TradeDeduplicator {

    private final boolean enabled;
    private final Cache<String, Boolean> claimed;
    private final Map<Exchange, Counter> duplicatesByExchange = new EnumMap<>(Exchange.class);
    private final Counter duplicatesUnknownExchange;

    public TradeDeduplicator(GatewayProperties properties, MeterRegistry meterRegistry) {
        GatewayProperties.Dedup config = properties.dedup();
        this.enabled = config.enabled();
        this.claimed = Caffeine.newBuilder()
                .maximumSize(config.maxEntries())
                .expireAfterWrite(config.window())
                .build();

        for (Exchange exchange : Exchange.values()) {
            duplicatesByExchange.put(exchange, Counter.builder("mdg.events.deduplicated")
                    .description("Trades dropped because the same trade was already delivered")
                    .tag("exchange", exchange.name())
                    .register(meterRegistry));
        }
        this.duplicatesUnknownExchange = Counter.builder("mdg.events.deduplicated")
                .tag("exchange", "UNKNOWN")
                .register(meterRegistry);

        Gauge.builder("mdg.dedup.cache.size", claimed, Cache::estimatedSize)
                .description("Trade ids currently remembered for deduplication")
                .register(meterRegistry);
    }

    /**
     * Atomically claims a trade for delivery.
     *
     * @return {@code true} if this is the first copy and the caller should deliver it;
     *         {@code false} if it is a duplicate and must be dropped
     */
    public boolean claim(CanonicalTradeEvent event) {
        if (!enabled) {
            return true;
        }
        boolean first = claimed.asMap().putIfAbsent(event.eventId(), Boolean.TRUE) == null;
        if (!first) {
            counterFor(event).increment();
        }
        return first;
    }

    /**
     * Forgets a claim whose delivery failed, so a later copy is not wrongly discarded.
     */
    public void release(CanonicalTradeEvent event) {
        if (enabled) {
            claimed.invalidate(event.eventId());
        }
    }

    /** Read-only check, for dry runs that must not change state. */
    public boolean isDuplicate(CanonicalTradeEvent event) {
        return enabled && claimed.getIfPresent(event.eventId()) != null;
    }

    private Counter counterFor(CanonicalTradeEvent event) {
        return Exchange.fromName(event.exchange())
                .map(duplicatesByExchange::get)
                .orElse(duplicatesUnknownExchange);
    }
}
