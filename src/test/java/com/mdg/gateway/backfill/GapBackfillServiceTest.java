package com.mdg.gateway.backfill;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.config.GatewayProperties;
import com.mdg.gateway.config.JacksonConfig;
import com.mdg.gateway.mapper.ExchangePayloadMapperImpl;
import com.mdg.gateway.mapper.SymbolNormalizer;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.service.TradeSequencer;
import com.mdg.gateway.support.FakeVenueRestServer;
import com.mdg.gateway.support.Fixtures;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Gap recovery against a local server that pages the way each real venue does.
 */
class GapBackfillServiceTest {

    private static final FakeVenueRestServer VENUE = new FakeVenueRestServer();

    @TempDir
    Path dir;

    private final ObjectMapper mapper = JacksonConfig.marketDataObjectMapper();
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        VENUE.reset();
        meterRegistry = new SimpleMeterRegistry();
    }

    @AfterAll
    static void stopServer() {
        VENUE.close();
    }

    private GatewayProperties properties(int maxGap) {
        GatewayProperties base = Fixtures.gatewayProperties();
        return new GatewayProperties(base.topics(), base.symbol(), base.producer(), base.dlq(), base.dedup(),
                base.lanes(), base.spill(),
                new GatewayProperties.Backfill(true, dir.resolve("positions.json"), maxGap, Duration.ofSeconds(5),
                        1000, VENUE.url(), VENUE.url(), VENUE.url()));
    }

    private GapBackfillService service(int maxGap) {
        return new GapBackfillService(properties(maxGap), HttpClient.newHttpClient(), mapper,
                new ExchangePayloadMapperImpl(new SymbolNormalizer(Fixtures.gatewayProperties())), meterRegistry);
    }

    /** The live trade that revealed the gap, and the gap before it. */
    private static CanonicalTradeEvent live(String exchange, String venueSymbol, long id) {
        return Fixtures.canonicalEvent().toBuilder()
                .exchange(exchange).venueSymbol(venueSymbol).tradeId(Long.toString(id))
                .eventId(CanonicalTradeEvent.deterministicEventId(exchange, venueSymbol, Long.toString(id)))
                .build();
    }

    private static TradeSequencer.Check gap(long lastDelivered, long nextSeen) {
        return new TradeSequencer.Check(TradeSequencer.Verdict.GAP, lastDelivered + 1, nextSeen - 1,
                Instant.ofEpochSecond(FakeVenueRestServer.EPOCH_SECONDS + lastDelivered));
    }

    private static List<String> ids(long from, long to) {
        return LongStream.rangeClosed(from, to).mapToObj(Long::toString).toList();
    }

    private double count(String name, String exchange) {
        return meterRegistry.counter(name, "exchange", exchange).count();
    }

    @Test
    @DisplayName("Binance: recovers the gap in order, as backfilled events with the live path's identity")
    void binance() {
        List<CanonicalTradeEvent> recovered = service(20_000).recover(live("BINANCE", "BTCUSDT", 105), gap(100, 105));

        assertThat(recovered).extracting(CanonicalTradeEvent::tradeId).containsExactly("101", "102", "103", "104");
        CanonicalTradeEvent first = recovered.getFirst();
        assertThat(first.backfilled()).isTrue();
        assertThat(first.exchange()).isEqualTo("BINANCE");
        assertThat(first.symbol()).isEqualTo("BTC-USD");
        assertThat(first.venueSymbol()).isEqualTo("BTCUSDT");
        assertThat(first.price()).isEqualByComparingTo("82858.8");
        assertThat(first.timestamp()).isEqualTo(Instant.ofEpochSecond(FakeVenueRestServer.EPOCH_SECONDS + 101));
        // Same id the socket would have produced, so a late live copy deduplicates against it.
        assertThat(first.eventId()).isEqualTo(CanonicalTradeEvent.deterministicEventId("BINANCE", "BTCUSDT", "101"));
        assertThat(first.rawPayload()).contains("\"id\":101");

        assertThat(VENUE.requests()).containsExactly("/api/v3/historicalTrades?symbol=BTCUSDT&fromId=101&limit=4");
        assertThat(count("mdg.backfill.gaps", "BINANCE")).isEqualTo(1.0);
        assertThat(count("mdg.backfill.trades.recovered", "BINANCE")).isEqualTo(4.0);
        assertThat(count("mdg.backfill.trades.missing", "BINANCE")).isZero();
    }

    @Test
    @DisplayName("Binance: pages forward through a gap larger than one response")
    void binancePages() {
        List<CanonicalTradeEvent> recovered = service(20_000).recover(live("BINANCE", "ETHUSDT", 3_501), gap(1_000, 3_501));

        assertThat(recovered).extracting(CanonicalTradeEvent::tradeId).containsExactlyElementsOf(ids(1_001, 3_500));
        assertThat(VENUE.requests()).hasSize(3);
    }

    @Test
    @DisplayName("Coinbase: pages backwards from the end of the gap, and still returns ascending order")
    void coinbase() {
        List<CanonicalTradeEvent> recovered = service(20_000).recover(live("COINBASE", "BTC-USD", 2_701), gap(200, 2_701));

        assertThat(recovered).extracting(CanonicalTradeEvent::tradeId).containsExactlyElementsOf(ids(201, 2_700));
        assertThat(VENUE.requests()).containsExactly(
                "/products/BTC-USD/trades?after=2701&limit=1000",
                "/products/BTC-USD/trades?after=1701&limit=1000",
                "/products/BTC-USD/trades?after=701&limit=500");
        CanonicalTradeEvent first = recovered.getFirst();
        assertThat(first.backfilled()).isTrue();
        // REST pads to 8 decimals; the canonical event must not ("82771", not "82771.00000000").
        assertThat(first.price()).isEqualTo(new BigDecimal("82771").stripTrailingZeros());
        assertThat(first.quantity().toPlainString()).isEqualTo("0.00005729");
    }

    @Test
    @DisplayName("Kraken: pages by time from just before the last delivered trade, following the cursor")
    void kraken() {
        VENUE.setPageSize(400);

        List<CanonicalTradeEvent> recovered = service(20_000).recover(live("KRAKEN", "BTC/USD", 1_501), gap(500, 1_501));

        assertThat(recovered).extracting(CanonicalTradeEvent::tradeId).containsExactlyElementsOf(ids(501, 1_500));
        assertThat(VENUE.requests()).hasSize(3);
        // The pair is URL-encoded, and the search starts 2 s before the last delivered trade.
        assertThat(VENUE.requests().getFirst()).isEqualTo(
                "/0/public/Trades?pair=BTC%2FUSD&since=" + (FakeVenueRestServer.EPOCH_SECONDS + 498) + "000000000");
        CanonicalTradeEvent first = recovered.getFirst();
        assertThat(first.symbol()).isEqualTo("BTC-USD");
        assertThat(first.price().toPlainString()).isEqualTo("82781.2");
        assertThat(first.timestamp()).isEqualTo(Instant.ofEpochSecond(FakeVenueRestServer.EPOCH_SECONDS + 501));
    }

    @Test
    @DisplayName("Kraken timestamps keep their sub-second precision")
    void krakenTimestampPrecision() {
        assertThat(KrakenBackfillClient.toInstant(new BigDecimal("1791625771.463717")))
                .isEqualTo(Instant.parse("2026-10-10T09:49:31.463717Z"));
        assertThat(KrakenBackfillClient.toInstant(new BigDecimal("1791625774.1477826")))
                .isEqualTo(Instant.ofEpochSecond(1791625774L, 147_782_600));
    }

    @Test
    @DisplayName("a gap longer than the limit recovers only its newest part and counts the rest as missing")
    void gapBeyondLimit() {
        List<CanonicalTradeEvent> recovered = service(50).recover(live("BINANCE", "BTCUSDT", 1_001), gap(0, 1_001));

        // The newest 50 join up with the live stream: one clean hole, not a hole in the middle.
        assertThat(recovered).extracting(CanonicalTradeEvent::tradeId).containsExactlyElementsOf(ids(951, 1_000));
        assertThat(count("mdg.backfill.trades.recovered", "BINANCE")).isEqualTo(50.0);
        assertThat(count("mdg.backfill.trades.missing", "BINANCE")).isEqualTo(950.0);
    }

    @Test
    @DisplayName("HTTP 429 stops the backfill and pauses it, instead of retrying into a ban")
    void rateLimitedBacksOff() {
        VENUE.failWith(429, "30");
        GapBackfillService service = service(20_000);

        assertThat(service.recover(live("BINANCE", "BTCUSDT", 105), gap(100, 105))).isEmpty();
        assertThat(count("mdg.backfill.rate.limited", "BINANCE")).isEqualTo(1.0);
        assertThat(count("mdg.backfill.trades.missing", "BINANCE")).isEqualTo(4.0);

        // The venue recovers, but we are still inside its Retry-After: no request is made.
        VENUE.failWith(0, null);
        assertThat(service.recover(live("BINANCE", "BTCUSDT", 110), gap(105, 110))).isEmpty();
        assertThat(VENUE.requests()).hasSize(1);

        // The cooldown is per venue: Coinbase is unaffected.
        assertThat(service.recover(live("COINBASE", "BTC-USD", 105), gap(100, 105))).hasSize(4);
    }

    @Test
    @DisplayName("a failure part-way keeps the pages already fetched")
    void partialFailureKeepsWhatWasFetched() {
        VENUE.failAfter(1);

        List<CanonicalTradeEvent> recovered = service(20_000).recover(live("BINANCE", "BTCUSDT", 2_501), gap(0, 2_501));

        assertThat(recovered).extracting(CanonicalTradeEvent::tradeId).containsExactlyElementsOf(ids(1, 1_000));
        assertThat(count("mdg.backfill.trades.missing", "BINANCE")).isEqualTo(1_500.0);
    }

    @Test
    @DisplayName("ids the venue itself does not have are reported missing, without looping")
    void venueHoles() {
        VENUE.removeTrades(102L, 103L);

        List<CanonicalTradeEvent> recovered = service(20_000).recover(live("BINANCE", "BTCUSDT", 106), gap(100, 106));

        assertThat(recovered).extracting(CanonicalTradeEvent::tradeId).containsExactly("101", "104", "105");
        assertThat(count("mdg.backfill.trades.missing", "BINANCE")).isEqualTo(2.0);
    }

    @Test
    @DisplayName("an unreachable venue yields nothing and never throws into the feed")
    void unreachableVenue() {
        VENUE.failWith(503, null);

        assertThat(service(20_000).recover(live("KRAKEN", "BTC/USD", 105), gap(100, 105))).isEmpty();
        assertThat(count("mdg.backfill.trades.missing", "KRAKEN")).isEqualTo(4.0);
    }
}
