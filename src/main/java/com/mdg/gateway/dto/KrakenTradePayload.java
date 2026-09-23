package com.mdg.gateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * One entry of Kraken WS v2's {@code trade} channel {@code data} array.
 *
 * <pre>
 * {"symbol":"BTC/USD","side":"buy","price":4136.4,"qty":0.23374249,
 *  "ord_type":"market","trade_id":0,"timestamp":"2022-12-25T09:30:59.123456Z"}
 * </pre>
 *
 * <p>Unlike Binance, Kraken v2 sends numerics as JSON <em>numbers</em>. Jackson parses
 * those into {@link BigDecimal} exactly (never via {@code double}) because the gateway
 * enables {@code USE_BIG_DECIMAL_FOR_FLOATS}; see {@code JacksonConfig}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record KrakenTradePayload(

        @JsonProperty("symbol") String symbol,

        @JsonProperty("side") String side,

        @JsonProperty("price") BigDecimal price,

        @JsonProperty("qty") BigDecimal quantity,

        @JsonProperty("ord_type") String orderType,

        @JsonProperty("trade_id") Long tradeId,

        @JsonProperty("timestamp") Instant timestamp) {
}
