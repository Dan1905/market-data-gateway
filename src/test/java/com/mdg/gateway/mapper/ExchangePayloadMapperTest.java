package com.mdg.gateway.mapper;

import com.mdg.gateway.dto.BinanceTradePayload;
import com.mdg.gateway.dto.CoinbaseTradePayload;
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
 * Exercises the committed {@code ExchangePayloadMapperImpl} directly — no Spring context,
 * so a failure here is a mapping defect and nothing else. These tests are also the drift
 * guard for the committed implementation: change the interface without regenerating and they
 * fail.
 */
class ExchangePayloadMapperTest {

    private final ExchangePayloadMapper mapper =
            new ExchangePayloadMapperImpl(new SymbolNormalizer(Fixtures.gatewayProperties()));

    private static BinanceTradePayload binance(Long tradeId, String symbol, BigDecimal price,
                                               BigDecimal qty, Long tradeTime) {
        return new BinanceTradePayload("trade", 1672515782136L, symbol, tradeId, price, qty, tradeTime, true);
    }

    @Nested
    @DisplayName("Binance")
    class Binance {

        @Test
        void mapsEveryCanonicalField() {
            BinanceTradePayload payload = binance(12345L, "BTCUSDT",
                    new BigDecimal("16580.01"), new BigDecimal("0.004"), 1672515782136L);

            CanonicalTradeEvent event = mapper.fromBinance(payload, false, Fixtures.BINANCE_TRADE);

            assertThat(event.exchange()).isEqualTo("BINANCE");
            assertThat(event.symbol()).isEqualTo("BTC-USD");
            assertThat(event.venueSymbol()).isEqualTo("BTCUSDT");
            assertThat(event.tradeId()).isEqualTo("12345");
            assertThat(event.price()).isEqualByComparingTo("16580.01");
            assertThat(event.quantity()).isEqualByComparingTo("0.004");
            assertThat(event.timestamp()).isEqualTo(Instant.ofEpochMilli(1672515782136L));
            assertThat(event.backfilled()).isFalse();
            assertThat(event.rawPayload()).isEqualTo(Fixtures.BINANCE_TRADE);
            assertThat(UUID.fromString(event.eventId())).isNotNull();
        }

        @Test
        void prefersTradeTimeOverEventTime() {
            BinanceTradePayload payload = new BinanceTradePayload(
                    "trade", 1672515799999L, "BTCUSDT", 1L,
                    BigDecimal.ONE, BigDecimal.ONE, 1672515782136L, false);

            assertThat(mapper.fromBinance(payload, false, "{}").timestamp())
                    .isEqualTo(Instant.ofEpochMilli(1672515782136L));
        }

        @Test
        void carriesTheBackfilledFlag() {
            BinanceTradePayload payload = binance(1L, "BTCUSDT", BigDecimal.ONE, BigDecimal.ONE, 1L);
            assertThat(mapper.fromBinance(payload, true, "{}").backfilled()).isTrue();
        }
    }

    @Nested
    @DisplayName("Coinbase market_trades")
    class Coinbase {

        @Test
        void mapsRealPerTradeSizeAsQuantity() {
            CoinbaseTradePayload payload = new CoinbaseTradePayload("BTC-USD", "1099158137",
                    new BigDecimal("84451.04"), new BigDecimal("0.00046022"),
                    Instant.parse("2026-09-27T16:42:06.745095Z"), "SELL");

            CanonicalTradeEvent event = mapper.fromCoinbase(payload, false, Fixtures.COINBASE_TRADE);

            assertThat(event.exchange()).isEqualTo("COINBASE");
            assertThat(event.symbol()).isEqualTo("BTC-USD");
            assertThat(event.venueSymbol()).isEqualTo("BTC-USD");
            assertThat(event.tradeId()).isEqualTo("1099158137");
            assertThat(event.price()).isEqualByComparingTo("84451.04");
            // The ticker channel used to put 24h rolling volume here; this is the real size.
            assertThat(event.quantity()).isEqualByComparingTo("0.00046022");
            assertThat(event.timestamp()).isEqualTo(Instant.parse("2026-09-27T16:42:06.745095Z"));
        }
    }

    @Nested
    @DisplayName("Kraken")
    class Kraken {

        @Test
        void mapsSlashedSymbolAndNumericPrice() {
            KrakenTradePayload payload = new KrakenTradePayload(
                    "BTC/USD", "buy", new BigDecimal("4136.4"), new BigDecimal("0.23374249"),
                    "market", 109253905L, Instant.parse("2022-12-25T09:30:59.123456Z"));

            CanonicalTradeEvent event = mapper.fromKraken(payload, false, Fixtures.KRAKEN_TRADE);

            assertThat(event.exchange()).isEqualTo("KRAKEN");
            assertThat(event.symbol()).isEqualTo("BTC-USD");
            assertThat(event.venueSymbol()).isEqualTo("BTC/USD");
            assertThat(event.tradeId()).isEqualTo("109253905");
            assertThat(event.price()).isEqualByComparingTo("4136.4");
            assertThat(event.quantity()).isEqualByComparingTo("0.23374249");
        }
    }

    @Nested
    @DisplayName("deterministic identity")
    class Identity {

        @Test
        @DisplayName("the same trade always gets the same eventId")
        void sameTradeSameId() {
            BinanceTradePayload payload = binance(777L, "BTCUSDT", BigDecimal.ONE, BigDecimal.ONE, 1L);

            // Live, again after a reconnect, from a backfill: one trade, one id. This is what
            // makes deduplication possible at all.
            String live = mapper.fromBinance(payload, false, "{\"live\":1}").eventId();
            String again = mapper.fromBinance(payload, false, "{\"again\":1}").eventId();
            String backfilled = mapper.fromBinance(payload, true, "{\"rest\":1}").eventId();

            assertThat(live).isEqualTo(again).isEqualTo(backfilled);
        }

        @Test
        @DisplayName("different trades get different ids")
        void differentTradesDifferentIds() {
            String a = mapper.fromBinance(binance(1L, "BTCUSDT", BigDecimal.ONE, BigDecimal.ONE, 1L), false, "{}").eventId();
            String b = mapper.fromBinance(binance(2L, "BTCUSDT", BigDecimal.ONE, BigDecimal.ONE, 1L), false, "{}").eventId();
            assertThat(a).isNotEqualTo(b);
        }

        @Test
        @DisplayName("keys on the VENUE symbol: BTCUSDT and BTCUSDC never collide")
        void venueSymbolPreventsStablecoinCollision() {
            // Both normalize to BTC-USD, but each Binance market has its own trade-id sequence,
            // so trade 42 on one and trade 42 on the other are different trades. Keyed on the
            // normalized symbol, the second would be dropped as a duplicate.
            CanonicalTradeEvent usdt = mapper.fromBinance(
                    binance(42L, "BTCUSDT", BigDecimal.ONE, BigDecimal.ONE, 1L), false, "{}");
            CanonicalTradeEvent usdc = mapper.fromBinance(
                    binance(42L, "BTCUSDC", BigDecimal.ONE, BigDecimal.ONE, 1L), false, "{}");

            assertThat(usdt.symbol()).isEqualTo(usdc.symbol()).isEqualTo("BTC-USD");
            assertThat(usdt.eventId()).isNotEqualTo(usdc.eventId());
        }

        @Test
        @DisplayName("the same numeric id on different venues never collides")
        void exchangeIsPartOfTheId() {
            String b = CanonicalTradeEvent.deterministicEventId("BINANCE", "BTC-USD", "5");
            String c = CanonicalTradeEvent.deterministicEventId("COINBASE", "BTC-USD", "5");
            assertThat(b).isNotEqualTo(c);
        }
    }

    @Nested
    @DisplayName("missing and unexpected fields")
    class InvalidInput {

        @Test
        void rejectsMissingPrice() {
            assertThatThrownBy(() -> mapper.fromBinance(binance(1L, "BTCUSDT", null, BigDecimal.ONE, 1L), false, "{}"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("price is required");
        }

        @Test
        void rejectsMissingQuantity() {
            assertThatThrownBy(() -> mapper.fromBinance(binance(1L, "BTCUSDT", BigDecimal.ONE, null, 1L), false, "{}"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("quantity is required");
        }

        @Test
        void rejectsMissingTimestamp() {
            KrakenTradePayload payload = new KrakenTradePayload(
                    "BTC/USD", "buy", BigDecimal.ONE, BigDecimal.ONE, "market", 1L, null);

            assertThatThrownBy(() -> mapper.fromKraken(payload, false, "{}"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("timestamp is required");
        }

        @Test
        void rejectsMissingSymbol() {
            // No symbol means no deterministic id either; whichever check fires first, the
            // frame is rejected rather than published with a hole in it.
            assertThatThrownBy(() -> mapper.fromBinance(binance(1L, null, BigDecimal.ONE, BigDecimal.ONE, 1L), false, "{}"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("is required");
        }

        @Test
        @DisplayName("a trade without a venue trade id cannot be identified, so it is rejected")
        void rejectsMissingTradeId() {
            assertThatThrownBy(() -> mapper.fromBinance(binance(null, "BTCUSDT", BigDecimal.ONE, BigDecimal.ONE, 1L), false, "{}"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("eventId is required");
        }

        @Test
        void rejectsNonPositivePrice() {
            assertThatThrownBy(() -> mapper.fromBinance(binance(1L, "BTCUSDT", BigDecimal.ZERO, BigDecimal.ONE, 1L), false, "{}"))
                    .hasMessageContaining("price must be positive");
            assertThatThrownBy(() -> mapper.fromBinance(binance(1L, "BTCUSDT", new BigDecimal("-1"), BigDecimal.ONE, 1L), false, "{}"))
                    .hasMessageContaining("price must be positive");
        }

        @Test
        void rejectsNegativeQuantityButAllowsZero() {
            assertThatThrownBy(() -> mapper.fromBinance(binance(1L, "BTCUSDT", BigDecimal.ONE, new BigDecimal("-0.5"), 1L), false, "{}"))
                    .hasMessageContaining("quantity must not be negative");

            assertThat(mapper.fromBinance(binance(1L, "BTCUSDT", BigDecimal.ONE, BigDecimal.ZERO, 1L), false, "{}")
                    .quantity()).isEqualByComparingTo("0");
        }
    }
}
