package com.mdg.gateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * One trade from Coinbase Advanced Trade's public {@code market_trades} channel.
 *
 * <pre>
 * {"product_id":"BTC-USD","trade_id":"1099158137","price":"84451.04",
 *  "size":"0.00046022","time":"2026-09-27T16:42:06.745095Z","side":"SELL"}
 * </pre>
 *
 * <p>This replaced the {@code ticker} channel. The ticker carried no trade id — so it could not
 * be deduplicated or gap-checked — and no per-trade size, which forced the canonical
 * {@code quantity} to hold a rolling 24h volume that meant something different from every other
 * venue's quantity. Both problems go away here: {@code trade_id} is sequential per product and
 * shares its id space with the public REST trades endpoint, and {@code size} is the real size.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CoinbaseTradePayload(

        @JsonProperty("product_id") String productId,

        /* Sent as a JSON string; sequential per product. */
        @JsonProperty("trade_id") String tradeId,

        @JsonProperty("price") BigDecimal price,

        @JsonProperty("size") BigDecimal size,

        @JsonProperty("time") Instant time,

        @JsonProperty("side") String side) {
}
