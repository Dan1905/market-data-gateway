package com.mdg.gateway.support;

import com.mdg.gateway.config.GatewayProperties;
import com.mdg.gateway.model.CanonicalTradeEvent;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

/**
 * Shared test data.
 *
 * <p>The venue payloads are copied from each exchange's published documentation rather than
 * invented, so a test failure means our parsing drifted — not that the fixture was wrong.
 */
public final class Fixtures {

    // ------------------------------------------------------------------
    // Binance — string numerics, epoch millis, concatenated symbol
    // ------------------------------------------------------------------

    public static final String BINANCE_TRADE = """
            {"e":"trade","E":1672515782136,"s":"BTCUSDT","t":12345,\
            "p":"16580.01","q":"0.004","T":1672515782136,"m":true,"M":true}""";

    /** Subscription acknowledgement — a control frame, not a failure. */
    public static final String BINANCE_SUBSCRIBE_ACK = """
            {"result":null,"id":1}""";

    /** A genuine trade frame missing its price. Must be dead-lettered. */
    public static final String BINANCE_TRADE_NO_PRICE = """
            {"e":"trade","E":1672515782136,"s":"BTCUSDT","t":12345,\
            "q":"0.004","T":1672515782136,"m":true}""";

    /** A trade frame with a nonsensical negative price. Must be dead-lettered. */
    public static final String BINANCE_TRADE_NEGATIVE_PRICE = """
            {"e":"trade","E":1672515782136,"s":"BTCUSDT","t":12345,\
            "p":"-1.00","q":"0.004","T":1672515782136,"m":true}""";

    public static final String MALFORMED_JSON = "{\"e\":\"trade\",\"p\":";

    // ------------------------------------------------------------------
    // Coinbase — nested envelope, batched tickers, ISO-8601 envelope time
    // ------------------------------------------------------------------

    public static final String COINBASE_TICKER = """
            {"channel":"ticker","client_id":"","timestamp":"2023-02-09T20:19:35.396251Z","sequence_num":0,\
            "events":[{"type":"update","tickers":[\
            {"type":"ticker","product_id":"BTC-USD","price":"21932.98","volume_24_h":"16038.28770938",\
            "low_24_h":"21835.29","high_24_h":"23011.18"}]}]}""";

    /** Two tickers in one frame — the fan-out case. */
    public static final String COINBASE_TICKER_BATCH = """
            {"channel":"ticker","timestamp":"2023-02-09T20:19:35.396251Z","sequence_num":1,\
            "events":[{"type":"update","tickers":[\
            {"type":"ticker","product_id":"BTC-USD","price":"21932.98","volume_24_h":"16038.28"},\
            {"type":"ticker","product_id":"ETH-USD","price":"1592.41","volume_24_h":"210330.12"}]}]}""";

    /** Subscription confirmation on the `subscriptions` channel — a control frame. */
    public static final String COINBASE_SUBSCRIPTIONS = """
            {"channel":"subscriptions","timestamp":"2023-02-09T20:19:35.396251Z",\
            "events":[{"subscriptions":{"ticker":["BTC-USD"]}}]}""";

    // ------------------------------------------------------------------
    // Kraken v2 — numeric price/qty, slashed symbol, control channels
    // ------------------------------------------------------------------

    public static final String KRAKEN_TRADE = """
            {"channel":"trade","type":"update","data":[\
            {"symbol":"BTC/USD","side":"buy","price":4136.4,"qty":0.23374249,\
            "ord_type":"market","trade_id":0,"timestamp":"2022-12-25T09:30:59.123456Z"}]}""";

    public static final String KRAKEN_HEARTBEAT = """
            {"channel":"heartbeat"}""";

    public static final String KRAKEN_SUBSCRIBE_ACK = """
            {"method":"subscribe","req_id":1,"result":{"channel":"trade","symbol":"BTC/USD"},"success":true}""";

    public static final String KRAKEN_ERROR = """
            {"error":"Subscription failed: unknown symbol","method":"subscribe","success":false}""";

    /** Trade frame with no timestamp. Must be dead-lettered. */
    public static final String KRAKEN_TRADE_NO_TIMESTAMP = """
            {"channel":"trade","type":"update","data":[\
            {"symbol":"BTC/USD","side":"buy","price":4136.4,"qty":0.23374249,"trade_id":1}]}""";

    // ------------------------------------------------------------------

    public static GatewayProperties gatewayProperties() {
        return gatewayProperties(true);
    }

    public static GatewayProperties gatewayProperties(boolean collapseStablecoins) {
        return new GatewayProperties(
                new GatewayProperties.Topics("normalized-market-data", "market-data-dlq", 1, (short) 1,
                        Duration.ofHours(24), Duration.ofDays(7)),
                new GatewayProperties.Symbol(collapseStablecoins),
                new GatewayProperties.Producer(Duration.ofSeconds(5)),
                new GatewayProperties.Dlq("test-replay", Duration.ofMinutes(1), 10));
    }

    public static CanonicalTradeEvent canonicalEvent() {
        return CanonicalTradeEvent.builder()
                .eventId("11111111-2222-3333-4444-555555555555")
                .exchange("BINANCE")
                .symbol("BTC-USD")
                .price(new BigDecimal("16580.01"))
                .quantity(new BigDecimal("0.004"))
                // Same epoch as BINANCE_TRADE's "T" field, so fixtures stay consistent.
                .timestamp(Instant.ofEpochMilli(1672515782136L))
                .rawPayload(BINANCE_TRADE)
                .build();
    }

    private Fixtures() {
    }
}
