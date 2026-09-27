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
    // Coinbase market_trades — nested envelope, batched trades, snapshot on subscribe
    // (shapes copied from live frames captured 2026-09-27)
    // ------------------------------------------------------------------

    public static final String COINBASE_TRADE = """
            {"channel":"market_trades","timestamp":"2026-09-27T16:42:07.238434983Z","sequence_num":3,\
            "events":[{"type":"update","trades":[\
            {"product_id":"BTC-USD","trade_id":"1099158138","price":"84451.05","size":"0.0215708",\
            "time":"2026-09-27T16:42:07.184962Z","side":"BUY"}]}]}""";

    /** Two trades for different products in one frame — the fan-out case. */
    public static final String COINBASE_TRADE_BATCH = """
            {"channel":"market_trades","timestamp":"2026-09-27T16:42:07.238434983Z","sequence_num":4,\
            "events":[{"type":"update","trades":[\
            {"product_id":"BTC-USD","trade_id":"1099158139","price":"84451.04","size":"0.00046022",\
            "time":"2026-09-27T16:42:07.300000Z","side":"SELL"},\
            {"product_id":"ETH-USD","trade_id":"845685175","price":"2686.65","size":"0.0215708",\
            "time":"2026-09-27T16:42:07.184962Z","side":"BUY"}]}]}""";

    /** The snapshot Coinbase sends on subscribe: recent trades, flagged as backfilled. */
    public static final String COINBASE_SNAPSHOT = """
            {"channel":"market_trades","timestamp":"2026-09-27T16:42:07.141291854Z","sequence_num":0,\
            "events":[{"type":"snapshot","trades":[\
            {"product_id":"BTC-USD","trade_id":"1099158137","price":"84451.04","size":"0.00046022",\
            "time":"2026-09-27T16:42:06.745095Z","side":"SELL"},\
            {"product_id":"BTC-USD","trade_id":"1099158136","price":"84451.04","size":"0.00224626",\
            "time":"2026-09-27T16:42:06.712987Z","side":"SELL"}]}]}""";

    /** Subscription confirmation on the `subscriptions` channel — a control frame. */
    public static final String COINBASE_SUBSCRIPTIONS = """
            {"channel":"subscriptions","timestamp":"2026-09-27T16:42:07.141590305Z","sequence_num":2,\
            "events":[{"subscriptions":{"market_trades":["BTC-USD"]}}]}""";

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

    // ------------------------------------------------------------------
    // Unique trades. Integration tests share one Spring context and therefore one dedup
    // cache: two tests publishing the same fixture trade would see the second one dropped as
    // a duplicate. These generate a fresh venue trade id on every call.
    // ------------------------------------------------------------------

    private static final java.util.concurrent.atomic.AtomicLong NEXT_TRADE_ID =
            new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis() * 1000);

    public static long nextTradeId() {
        return NEXT_TRADE_ID.incrementAndGet();
    }

    public static String binanceTrade(long tradeId) {
        return "{\"e\":\"trade\",\"E\":1672515782136,\"s\":\"BTCUSDT\",\"t\":" + tradeId
                + ",\"p\":\"16580.01\",\"q\":\"0.004\",\"T\":1672515782136,\"m\":true,\"M\":true}";
    }

    public static String uniqueBinanceTrade() {
        return binanceTrade(nextTradeId());
    }

    public static String uniqueKrakenTrade() {
        return "{\"channel\":\"trade\",\"type\":\"update\",\"data\":[{\"symbol\":\"BTC/USD\","
                + "\"side\":\"buy\",\"price\":4136.4,\"qty\":0.23374249,\"ord_type\":\"market\","
                + "\"trade_id\":" + nextTradeId() + ",\"timestamp\":\"2022-12-25T09:30:59.123456Z\"}]}";
    }

    public static String uniqueCoinbaseTrade() {
        return "{\"channel\":\"market_trades\",\"timestamp\":\"2026-09-27T16:42:07.238434983Z\","
                + "\"sequence_num\":3,\"events\":[{\"type\":\"update\",\"trades\":[{\"product_id\":\"BTC-USD\","
                + "\"trade_id\":\"" + nextTradeId() + "\",\"price\":\"84451.05\",\"size\":\"0.0215708\","
                + "\"time\":\"2026-09-27T16:42:07.184962Z\",\"side\":\"BUY\"}]}]}";
    }

    public static GatewayProperties gatewayProperties() {
        return gatewayProperties(true);
    }

    public static GatewayProperties gatewayProperties(boolean collapseStablecoins) {
        return new GatewayProperties(
                new GatewayProperties.Topics("normalized-market-data", "market-data-dlq", 1, (short) 1,
                        Duration.ofHours(24), Duration.ofDays(7)),
                new GatewayProperties.Symbol(collapseStablecoins),
                new GatewayProperties.Producer(Duration.ofSeconds(5)),
                new GatewayProperties.Dlq("test-replay", Duration.ofMinutes(1), 10),
                new GatewayProperties.Dedup(true, 10_000, Duration.ofMinutes(10)),
                new GatewayProperties.Lanes(1000, Duration.ofSeconds(5)));
    }

    public static CanonicalTradeEvent canonicalEvent() {
        return CanonicalTradeEvent.builder()
                .eventId(CanonicalTradeEvent.deterministicEventId("BINANCE", "BTCUSDT", "12345"))
                .exchange("BINANCE")
                .symbol("BTC-USD")
                .venueSymbol("BTCUSDT")
                .tradeId("12345")
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
