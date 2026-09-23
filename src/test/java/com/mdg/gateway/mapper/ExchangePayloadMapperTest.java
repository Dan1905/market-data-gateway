package com.mdg.gateway.mapper;

import com.mdg.gateway.dto.BinanceTradePayload;
import com.mdg.gateway.dto.CoinbaseTickerPayload;
import com.mdg.gateway.dto.KrakenTradePayload;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.support.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the generated {@code ExchangePayloadMapperImpl} directly — no Spring context,
 * so a failure here is a mapping defect and nothing else.
 */
class ExchangePayloadMapperTest {

    private final ExchangePayloadMapper mapper =
            new ExchangePayloadMapperImpl(new SymbolNormalizer(Fixtures.gatewayProperties()));

    @Nested
    @DisplayName("Binance")
    class Binance {

        @Test
        void mapsEveryCanonicalField() {
            BinanceTradePayload payload = new BinanceTradePayload(
                    "trade", 1672515782136L, "BTCUSDT", 12345L,
                    new BigDecimal("16580.01"), new BigDecimal("0.004"), 1672515782136L, true);

            CanonicalTradeEvent event = mapper.fromBinance(payload, Fixtures.BINANCE_TRADE);

            assertThat(event.exchange()).isEqualTo("BINANCE");
            assertThat(event.symbol()).isEqualTo("BTC-USD");
            assertThat(event.price()).isEqualByComparingTo("16580.01");
            assertThat(event.quantity()).isEqualByComparingTo("0.004");
            assertThat(event.timestamp()).isEqualTo(Instant.ofEpochMilli(1672515782136L));
            assertThat(event.rawPayload()).isEqualTo(Fixtures.BINANCE_TRADE);
            assertThat(UUID.fromString(event.eventId())).isNotNull();
        }

        @Test
        void prefersTradeTimeOverEventTime() {
            // E (event time) and T (trade time) differ under load; the trade time is the
            // one that describes when the fill actually happened.
            BinanceTradePayload payload = new BinanceTradePayload(
                    "trade", 1672515799999L, "BTCUSDT", 1L,
                    BigDecimal.ONE, BigDecimal.ONE, 1672515782136L, false);

            assertThat(mapper.fromBinance(payload, "{}").timestamp())
                    .isEqualTo(Instant.ofEpochMilli(1672515782136L));
        }

        @Test
        void assignsAFreshEventIdPerCall() {
            BinanceTradePayload payload = new BinanceTradePayload(
                    "trade", 1L, "BTCUSDT", 1L, BigDecimal.ONE, BigDecimal.ONE, 1L, false);

            // The venue trade id is reused across symbols, so canonical identity must not
            // be derived from it.
            assertThat(mapper.fromBinance(payload, "{}").eventId())
                    .isNotEqualTo(mapper.fromBinance(payload, "{}").eventId());
        }
    }

    @Nested
    @DisplayName("Coinbase")
    class Coinbase {

        @Test
        void mapsTickerWithTwentyFourHourVolumeAsQuantity() {
            CoinbaseTickerPayload payload = new CoinbaseTickerPayload(
                    "ticker", "BTC-USD", new BigDecimal("21932.98"), new BigDecimal("16038.28770938"),
                    new BigDecimal("21932.00"), new BigDecimal("21933.50"),
                    Instant.parse("2023-02-09T20:19:35.396251Z"));

            CanonicalTradeEvent event = mapper.fromCoinbase(payload, Fixtures.COINBASE_TICKER);

            assertThat(event.exchange()).isEqualTo("COINBASE");
            assertThat(event.symbol()).isEqualTo("BTC-USD");
            assertThat(event.price()).isEqualByComparingTo("21932.98");
            assertThat(event.quantity()).isEqualByComparingTo("16038.28770938");
            assertThat(event.timestamp()).isEqualTo(Instant.parse("2023-02-09T20:19:35.396251Z"));
        }

        @Test
        void withTimestampLiftsEnvelopeTimeOnlyWhenTheTickerHasNone() {
            CoinbaseTickerPayload bare = new CoinbaseTickerPayload(
                    "ticker", "BTC-USD", BigDecimal.TEN, BigDecimal.ONE, null, null, null);
            Instant envelopeTime = Instant.parse("2023-02-09T20:19:35Z");

            assertThat(bare.withTimestamp(envelopeTime).timestamp()).isEqualTo(envelopeTime);

            Instant ownTime = Instant.parse("2020-01-01T00:00:00Z");
            CoinbaseTickerPayload stamped = new CoinbaseTickerPayload(
                    "ticker", "BTC-USD", BigDecimal.TEN, BigDecimal.ONE, null, null, ownTime);
            assertThat(stamped.withTimestamp(envelopeTime).timestamp()).isEqualTo(ownTime);
        }
    }

    @Nested
    @DisplayName("Kraken")
    class Kraken {

        @Test
        void mapsSlashedSymbolAndNumericPrice() {
            KrakenTradePayload payload = new KrakenTradePayload(
                    "BTC/USD", "buy", new BigDecimal("4136.4"), new BigDecimal("0.23374249"),
                    "market", 0L, Instant.parse("2022-12-25T09:30:59.123456Z"));

            CanonicalTradeEvent event = mapper.fromKraken(payload, Fixtures.KRAKEN_TRADE);

            assertThat(event.exchange()).isEqualTo("KRAKEN");
            assertThat(event.symbol()).isEqualTo("BTC-USD");
            assertThat(event.price()).isEqualByComparingTo("4136.4");
            assertThat(event.quantity()).isEqualByComparingTo("0.23374249");
        }
    }

    @Nested
    @DisplayName("missing and unexpected fields")
    class InvalidInput {

        @Test
        void rejectsMissingPrice() {
            BinanceTradePayload payload = new BinanceTradePayload(
                    "trade", 1L, "BTCUSDT", 1L, null, BigDecimal.ONE, 1L, false);

            assertThatThrownBy(() -> mapper.fromBinance(payload, "{}"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("price is required");
        }

        @Test
        void rejectsMissingQuantity() {
            BinanceTradePayload payload = new BinanceTradePayload(
                    "trade", 1L, "BTCUSDT", 1L, BigDecimal.ONE, null, 1L, false);

            assertThatThrownBy(() -> mapper.fromBinance(payload, "{}"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("quantity is required");
        }

        @Test
        void rejectsMissingTimestamp() {
            KrakenTradePayload payload = new KrakenTradePayload(
                    "BTC/USD", "buy", BigDecimal.ONE, BigDecimal.ONE, "market", 1L, null);

            assertThatThrownBy(() -> mapper.fromKraken(payload, "{}"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("timestamp is required");
        }

        @Test
        void rejectsMissingSymbol() {
            BinanceTradePayload payload = new BinanceTradePayload(
                    "trade", 1L, null, 1L, BigDecimal.ONE, BigDecimal.ONE, 1L, false);

            assertThatThrownBy(() -> mapper.fromBinance(payload, "{}"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("symbol is required");
        }

        @Test
        void rejectsNonPositivePrice() {
            BinanceTradePayload zero = new BinanceTradePayload(
                    "trade", 1L, "BTCUSDT", 1L, BigDecimal.ZERO, BigDecimal.ONE, 1L, false);
            BinanceTradePayload negative = new BinanceTradePayload(
                    "trade", 1L, "BTCUSDT", 1L, new BigDecimal("-1"), BigDecimal.ONE, 1L, false);

            assertThatThrownBy(() -> mapper.fromBinance(zero, "{}"))
                    .hasMessageContaining("price must be positive");
            assertThatThrownBy(() -> mapper.fromBinance(negative, "{}"))
                    .hasMessageContaining("price must be positive");
        }

        @Test
        void rejectsNegativeQuantityButAllowsZero() {
            BinanceTradePayload negative = new BinanceTradePayload(
                    "trade", 1L, "BTCUSDT", 1L, BigDecimal.ONE, new BigDecimal("-0.5"), 1L, false);
            assertThatThrownBy(() -> mapper.fromBinance(negative, "{}"))
                    .hasMessageContaining("quantity must not be negative");

            // A zero-volume ticker snapshot is legitimate on an illiquid pair.
            BinanceTradePayload zero = new BinanceTradePayload(
                    "trade", 1L, "BTCUSDT", 1L, BigDecimal.ONE, BigDecimal.ZERO, 1L, false);
            assertThat(mapper.fromBinance(zero, "{}").quantity()).isEqualByComparingTo("0");
        }

        @Test
        void returnsNullWhenBothSourcesAreAbsent() {
            assertThat(mapper.fromBinance(null, null)).isNull();
        }
    }
}
