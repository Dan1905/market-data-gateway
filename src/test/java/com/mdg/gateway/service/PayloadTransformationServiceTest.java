package com.mdg.gateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.config.JacksonConfig;
import com.mdg.gateway.exception.PayloadParsingException;
import com.mdg.gateway.mapper.ExchangePayloadMapperImpl;
import com.mdg.gateway.mapper.SymbolNormalizer;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.support.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PayloadTransformationServiceTest {

    private final ObjectMapper objectMapper = JacksonConfig.marketDataObjectMapper();

    private final PayloadTransformationService service = new PayloadTransformationService(
            objectMapper, new ExchangePayloadMapperImpl(new SymbolNormalizer(Fixtures.gatewayProperties())));

    @Nested
    @DisplayName("happy path")
    class HappyPath {

        @Test
        void normalizesBinanceTrade() {
            List<CanonicalTradeEvent> events = service.transform(Exchange.BINANCE, Fixtures.BINANCE_TRADE);

            assertThat(events).singleElement().satisfies(event -> {
                assertThat(event.exchange()).isEqualTo("BINANCE");
                assertThat(event.symbol()).isEqualTo("BTC-USD");
                assertThat(event.price()).isEqualByComparingTo("16580.01");
                assertThat(event.rawPayload()).isEqualTo(Fixtures.BINANCE_TRADE);
            });
        }

        @Test
        void normalizesCoinbaseTicker() {
            List<CanonicalTradeEvent> events = service.transform(Exchange.COINBASE, Fixtures.COINBASE_TICKER);

            assertThat(events).singleElement().satisfies(event -> {
                assertThat(event.exchange()).isEqualTo("COINBASE");
                assertThat(event.symbol()).isEqualTo("BTC-USD");
                assertThat(event.price()).isEqualByComparingTo("21932.98");
            });
        }

        @Test
        void fansOutBatchedCoinbaseTickers() {
            List<CanonicalTradeEvent> events =
                    service.transform(Exchange.COINBASE, Fixtures.COINBASE_TICKER_BATCH);

            assertThat(events).hasSize(2);
            assertThat(events).extracting(CanonicalTradeEvent::symbol)
                    .containsExactly("BTC-USD", "ETH-USD");
            // Every fanned-out event keeps the whole original frame for audit.
            assertThat(events).allSatisfy(event ->
                    assertThat(event.rawPayload()).isEqualTo(Fixtures.COINBASE_TICKER_BATCH));
        }

        @Test
        void normalizesKrakenTrade() {
            List<CanonicalTradeEvent> events = service.transform(Exchange.KRAKEN, Fixtures.KRAKEN_TRADE);

            assertThat(events).singleElement().satisfies(event -> {
                assertThat(event.exchange()).isEqualTo("KRAKEN");
                assertThat(event.symbol()).isEqualTo("BTC-USD");
                assertThat(event.quantity()).isEqualByComparingTo("0.23374249");
            });
        }

        /**
         * Kraken sends price/qty as JSON numbers. Without USE_BIG_DECIMAL_FOR_FLOATS these
         * round-trip through {@code double} and lose their exact decimal value before they
         * ever reach BigDecimal — a silent precision bug in a financial pipeline.
         */
        @Test
        void parsesKrakenNumericsWithoutDoublePrecisionLoss() {
            CanonicalTradeEvent event =
                    service.transform(Exchange.KRAKEN, Fixtures.KRAKEN_TRADE).getFirst();

            assertThat(event.quantity()).isEqualTo(new BigDecimal("0.23374249"));
            assertThat(event.quantity().toPlainString()).isEqualTo("0.23374249");
            assertThat(event.price().toPlainString()).isEqualTo("4136.4");
        }
    }

    @Nested
    @DisplayName("control frames are skipped, never dead-lettered")
    class ControlFrames {

        /**
         * The single most important behaviour in this class. Venues multiplex heartbeats
         * and acknowledgements onto the market data socket; dead-lettering them buries the
         * one genuinely corrupt frame under thousands of heartbeats within an hour.
         */
        @Test
        void skipsBinanceSubscriptionAck() {
            assertThat(service.transform(Exchange.BINANCE, Fixtures.BINANCE_SUBSCRIBE_ACK)).isEmpty();
        }

        @Test
        void skipsCoinbaseSubscriptionConfirmation() {
            assertThat(service.transform(Exchange.COINBASE, Fixtures.COINBASE_SUBSCRIPTIONS)).isEmpty();
        }

        @Test
        void skipsKrakenHeartbeatAndAck() {
            assertThat(service.transform(Exchange.KRAKEN, Fixtures.KRAKEN_HEARTBEAT)).isEmpty();
            assertThat(service.transform(Exchange.KRAKEN, Fixtures.KRAKEN_SUBSCRIBE_ACK)).isEmpty();
        }

        @Test
        void skipsKrakenErrorFrameWithoutDeadLettering() {
            // Our subscription was rejected — an operational problem to alert on, but the
            // frame itself is not corrupt data to replay later.
            assertThat(service.transform(Exchange.KRAKEN, Fixtures.KRAKEN_ERROR)).isEmpty();
        }

        @Test
        void skipsCoinbaseTickerFrameWithNoEntries() {
            String empty = """
                    {"channel":"ticker","timestamp":"2023-02-09T20:19:35Z","events":[]}""";
            assertThat(service.transform(Exchange.COINBASE, empty)).isEmpty();
        }
    }

    @Nested
    @DisplayName("failures become PayloadParsingException")
    class Failures {

        @Test
        void rejectsMalformedJson() {
            assertThatThrownBy(() -> service.transform(Exchange.BINANCE, Fixtures.MALFORMED_JSON))
                    .isInstanceOf(PayloadParsingException.class)
                    .hasMessageContaining("Malformed BINANCE JSON")
                    .extracting(ex -> ((PayloadParsingException) ex).getRawPayload())
                    .isEqualTo(Fixtures.MALFORMED_JSON);
        }

        @Test
        void rejectsTradeFrameMissingPrice() {
            assertThatThrownBy(() -> service.transform(Exchange.BINANCE, Fixtures.BINANCE_TRADE_NO_PRICE))
                    .isInstanceOf(PayloadParsingException.class)
                    .hasMessageContaining("price is required");
        }

        @Test
        void rejectsNegativePrice() {
            assertThatThrownBy(() -> service.transform(Exchange.BINANCE, Fixtures.BINANCE_TRADE_NEGATIVE_PRICE))
                    .isInstanceOf(PayloadParsingException.class)
                    .hasMessageContaining("price must be positive");
        }

        @Test
        void rejectsKrakenTradeMissingTimestamp() {
            assertThatThrownBy(() -> service.transform(Exchange.KRAKEN, Fixtures.KRAKEN_TRADE_NO_TIMESTAMP))
                    .isInstanceOf(PayloadParsingException.class)
                    .hasMessageContaining("timestamp is required");
        }

        @ParameterizedTest
        @ValueSource(strings = {"", "   "})
        void rejectsEmptyFrame(String frame) {
            assertThatThrownBy(() -> service.transform(Exchange.BINANCE, frame))
                    .isInstanceOf(PayloadParsingException.class)
                    .hasMessageContaining("Empty frame");
        }

        @Test
        void rejectsNullFrame() {
            assertThatThrownBy(() -> service.transform(Exchange.KRAKEN, null))
                    .isInstanceOf(PayloadParsingException.class);
        }

        @Test
        void rejectsUnknownSourceExchange() {
            assertThatThrownBy(() -> service.transform(null, Fixtures.BINANCE_TRADE))
                    .isInstanceOf(PayloadParsingException.class)
                    .hasMessageContaining("no source exchange");
        }

        /** Every venue must fail the same way on garbage, so the DLQ reads consistently. */
        @ParameterizedTest
        @EnumSource(Exchange.class)
        void everyVenueRejectsNonJson(Exchange exchange) {
            assertThatThrownBy(() -> service.transform(exchange, "not json at all"))
                    .isInstanceOf(PayloadParsingException.class)
                    .satisfies(ex -> assertThat(((PayloadParsingException) ex).getExchange()).isEqualTo(exchange));
        }
    }

    @Nested
    @DisplayName("unexpected fields")
    class UnexpectedFields {

        @Test
        void toleratesFieldsTheVenueAddedAfterThisCodeWasWritten() {
            String withNewFields = """
                    {"e":"trade","E":1672515782136,"s":"BTCUSDT","t":12345,"p":"16580.01",\
                    "q":"0.004","T":1672515782136,"m":true,"M":true,\
                    "someBrandNewField":"surprise","nested":{"a":1}}""";

            // Venues add fields without notice. Failing on them would dead-letter an entire
            // healthy feed the day an exchange ships a release.
            assertThat(service.transform(Exchange.BINANCE, withNewFields)).hasSize(1);
        }
    }
}
