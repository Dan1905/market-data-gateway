package com.mdg.gateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * One entry of Coinbase Advanced Trade's {@code ticker} channel.
 *
 * <pre>
 * {"type":"ticker","product_id":"BTC-USD","price":"21932.98",
 *  "volume_24_h":"16038.28","best_bid":"21932.00","best_ask":"21933.50"}
 * </pre>
 *
 * <p><b>Semantic caveat worth knowing:</b> the ticker channel carries no per-trade size —
 * {@code volume_24_h} is a rolling 24h aggregate. The gateway maps it into the canonical
 * {@code quantity} so the schema stays uniform, but a consumer summing quantity across
 * venues would be adding Binance trade sizes to a Coinbase daily total. Switch this venue
 * to the {@code market_trades} channel if you need true per-trade size.
 *
 * <p>{@code timestamp} is not present on the ticker object itself; the client lifts it from
 * the enclosing {@link CoinbaseTickerEnvelope} so the mapper sees one flat shape.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CoinbaseTickerPayload(

        @JsonProperty("type") String type,

        @JsonProperty("product_id") String productId,

        @JsonProperty("price") BigDecimal price,

        @JsonProperty("volume_24_h") BigDecimal volume24h,

        @JsonProperty("best_bid") BigDecimal bestBid,

        @JsonProperty("best_ask") BigDecimal bestAsk,

        /** Lifted from the envelope by {@code CoinbaseWebSocketClient}; not on the wire here. */
        @JsonProperty("timestamp") Instant timestamp) {

    /** Returns a copy carrying the envelope timestamp, unless the frame already had one. */
    public CoinbaseTickerPayload withTimestamp(Instant envelopeTimestamp) {
        return timestamp != null
                ? this
                : new CoinbaseTickerPayload(type, productId, price, volume24h, bestBid, bestAsk, envelopeTimestamp);
    }
}
