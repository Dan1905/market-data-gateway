package com.mdg.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.service.MarketDataIngestionService;
import com.mdg.gateway.support.AbstractKafkaIT;
import com.mdg.gateway.support.FakeVenueRestServer;
import com.mdg.gateway.support.Fixtures;
import com.mdg.gateway.support.KafkaTestConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * "No data loss across a disconnect", end to end: frames in, real broker out, with the
 * venue's REST API played by a local server.
 *
 * <p>Its own context, because gap recovery is switched off for every other test (their
 * fixtures reuse trade ids, which a sequencer would rightly read as replays).
 */
@DirtiesContext
class GapBackfillIT extends AbstractKafkaIT {

    private static final FakeVenueRestServer VENUE = new FakeVenueRestServer();

    @DynamicPropertySource
    static void backfillAgainstTheFake(DynamicPropertyRegistry registry) {
        registry.add("gateway.backfill.enabled", () -> "true");
        registry.add("gateway.exchanges.binance.rest-url", () -> VENUE.url().toString());
        registry.add("gateway.exchanges.coinbase.rest-url", () -> VENUE.url().toString());
        registry.add("gateway.exchanges.kraken.rest-url", () -> VENUE.url().toString());
    }

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private MarketDataIngestionService ingestionService;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    @DisplayName("trades missed during a disconnect are fetched and land on the topic in order, before the live one")
    void missedTradesAreRecoveredInOrder() throws Exception {
        try (KafkaTestConsumer consumer = tail(NORMALIZED_TOPIC)) {
            // Live, live ... socket drops, 40 trades happen ... reconnects at 5043.
            ingestionService.ingest(Exchange.BINANCE, Fixtures.binanceTrade(5_001));
            ingestionService.ingest(Exchange.BINANCE, Fixtures.binanceTrade(5_002));
            ingestionService.ingest(Exchange.BINANCE, Fixtures.binanceTrade(5_043));
            ingestionService.ingest(Exchange.BINANCE, Fixtures.binanceTrade(5_044));

            List<JsonNode> events = new ArrayList<>();
            for (ConsumerRecord<String, String> record : consumer.awaitAtLeast(44, Duration.ofSeconds(30))) {
                events.add(objectMapper.readTree(record.value()));
            }

            // Complete and in order: the consumer cannot tell there was a disconnect...
            List<String> expected = new ArrayList<>();
            for (int id = 5_001; id <= 5_044; id++) {
                expected.add(Integer.toString(id));
            }
            assertThat(events).extracting(e -> e.get("tradeId").asText()).containsExactlyElementsOf(expected);

            // ...except by the flag on the 40 that came over REST.
            assertThat(events).filteredOn(e -> e.get("backfilled").asBoolean())
                    .extracting(e -> e.get("tradeId").asInt())
                    .hasSize(40).allMatch(id -> id >= 5_003 && id <= 5_042);
            JsonNode recovered = events.get(2);
            assertThat(recovered.get("symbol").asText()).isEqualTo("BTC-USD");
            assertThat(recovered.get("eventId").asText()).isEqualTo(
                    com.mdg.gateway.model.CanonicalTradeEvent.deterministicEventId("BINANCE", "BTCUSDT", "5003"));

            // The venue re-sends recent trades after the reconnect: nothing new reaches the topic.
            ingestionService.ingest(Exchange.BINANCE, Fixtures.binanceTrade(5_040));
            ingestionService.ingest(Exchange.BINANCE, Fixtures.binanceTrade(5_044));
            assertThat(consumer.drainFor(Duration.ofSeconds(2))).isEmpty();
        }

        assertThat(meterRegistry.counter("mdg.backfill.gaps", "exchange", "BINANCE").count()).isEqualTo(1.0);
        assertThat(meterRegistry.counter("mdg.backfill.trades.recovered", "exchange", "BINANCE").count()).isEqualTo(40.0);
        assertThat(meterRegistry.counter("mdg.backfill.trades.missing", "exchange", "BINANCE").count()).isZero();
        assertThat(VENUE.requests()).containsExactly("/api/v3/historicalTrades?symbol=BTCUSDT&fromId=5003&limit=40");
    }
}
