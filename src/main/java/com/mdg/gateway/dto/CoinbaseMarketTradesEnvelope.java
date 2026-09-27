package com.mdg.gateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;

/**
 * Outer Coinbase Advanced Trade frame for the {@code market_trades} channel.
 *
 * <pre>
 * {"channel":"market_trades","timestamp":"...","sequence_num":3,
 *  "events":[{"type":"update","trades":[ ... ]}]}
 * </pre>
 *
 * <p>On subscribe Coinbase first sends an event of type {@code snapshot} carrying recent
 * trades, then {@code update} events. Snapshot trades may already have been published before a
 * reconnect, so they are flagged {@code backfilled} and rely on deduplication to drop overlaps.
 * The same socket also carries {@code subscriptions} and {@code heartbeats} channel frames,
 * which are control traffic and are skipped.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CoinbaseMarketTradesEnvelope(

        @JsonProperty("channel") String channel,

        @JsonProperty("timestamp") Instant timestamp,

        @JsonProperty("sequence_num") Long sequenceNum,

        @JsonProperty("events") List<Event> events) {

    public static final String CHANNEL_MARKET_TRADES = "market_trades";
    public static final String EVENT_SNAPSHOT = "snapshot";

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Event(

            @JsonProperty("type") String type,

            @JsonProperty("trades") List<CoinbaseTradePayload> trades) {

        public boolean isSnapshot() {
            return EVENT_SNAPSHOT.equals(type);
        }
    }

    public boolean isMarketTrades() {
        return CHANNEL_MARKET_TRADES.equals(channel);
    }

    public List<Event> safeEvents() {
        return events == null ? List.of() : events;
    }
}
