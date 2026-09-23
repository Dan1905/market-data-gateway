package com.mdg.gateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Outer Kraken WS v2 frame.
 *
 * <pre>
 * {"channel":"trade","type":"update","data":[ ... ]}
 * </pre>
 *
 * <p>Kraken multiplexes control traffic on the same socket — {@code channel:"heartbeat"},
 * {@code channel:"status"}, and {@code method:"subscribe"} acknowledgements all arrive
 * here. Those are <em>not</em> failures and must never reach the DLQ, so the transform
 * stage filters on {@link #isTrade()} before attempting to map.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record KrakenMessageEnvelope(

        @JsonProperty("channel") String channel,

        /** {@code snapshot} on subscribe, {@code update} thereafter. */
        @JsonProperty("type") String type,

        /** Present on acknowledgement frames, e.g. {@code "subscribe"}. */
        @JsonProperty("method") String method,

        @JsonProperty("success") Boolean success,

        @JsonProperty("error") String error,

        @JsonProperty("data") List<KrakenTradePayload> data) {

    public static final String CHANNEL_TRADE = "trade";

    public boolean isTrade() {
        return CHANNEL_TRADE.equals(channel);
    }

    public List<KrakenTradePayload> safeData() {
        return data == null ? List.of() : data;
    }
}
