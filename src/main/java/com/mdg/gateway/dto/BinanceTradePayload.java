package com.mdg.gateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

/**
 * Binance {@code <symbol>@trade} frame.
 *
 * <pre>
 * {"e":"trade","E":1672515782136,"s":"BTCUSDT","t":12345,
 *  "p":"16580.01","q":"0.004","T":1672515782136,"m":true,"M":true}
 * </pre>
 *
 * <p>Binance sends every numeric as a JSON <em>string</em> to avoid double precision loss;
 * Jackson coerces those straight into {@link BigDecimal}, which is why we never touch
 * {@code double} anywhere in this pipeline.
 *
 * <p>Boxed {@code Long}/{@code BigDecimal} rather than primitives on purpose: a missing
 * field must arrive as {@code null} so the canonical model's invariants can reject it,
 * instead of being silently defaulted to 0.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BinanceTradePayload(

        @JsonProperty("e") String eventType,

        @JsonProperty("E") Long eventTime,

        @JsonProperty("s") String symbol,

        @JsonProperty("t") Long tradeId,

        @JsonProperty("p") BigDecimal price,

        @JsonProperty("q") BigDecimal quantity,

        /** Trade execution time (ms epoch) — preferred over {@code eventTime} for the canonical timestamp. */
        @JsonProperty("T") Long tradeTime,

        /** True when the buyer is the maker, i.e. the aggressor sold. */
        @JsonProperty("m") Boolean buyerIsMaker) {

    public static final String EVENT_TYPE_TRADE = "trade";

    public boolean isTrade() {
        return EVENT_TYPE_TRADE.equals(eventType);
    }
}
