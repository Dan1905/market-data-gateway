package com.mdg.gateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;

/**
 * Outer Coinbase Advanced Trade frame.
 *
 * <pre>
 * {"channel":"ticker","timestamp":"2023-02-09T20:19:35.396Z","sequence_num":0,
 *  "events":[{"type":"snapshot","tickers":[ ... ]}]}
 * </pre>
 *
 * <p>Coinbase batches: one frame carries N events, each carrying N tickers. The gateway
 * fans this out into individual canonical events — which is why the transform stage
 * returns a {@code List} rather than a single event.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CoinbaseTickerEnvelope(

        @JsonProperty("channel") String channel,

        @JsonProperty("timestamp") Instant timestamp,

        @JsonProperty("sequence_num") Long sequenceNum,

        @JsonProperty("events") List<Event> events) {

    public static final String CHANNEL_TICKER = "ticker";

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Event(

            @JsonProperty("type") String type,

            @JsonProperty("tickers") List<CoinbaseTickerPayload> tickers) {
    }

    public boolean isTicker() {
        return CHANNEL_TICKER.equals(channel);
    }

    public List<Event> safeEvents() {
        return events == null ? List.of() : events;
    }
}
