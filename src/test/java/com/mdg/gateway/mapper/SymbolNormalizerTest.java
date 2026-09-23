package com.mdg.gateway.mapper;

import com.mdg.gateway.support.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class SymbolNormalizerTest {

    private final SymbolNormalizer normalizer = new SymbolNormalizer(Fixtures.gatewayProperties());

    @Nested
    @DisplayName("concatenated venue symbols (Binance)")
    class Concatenated {

        @ParameterizedTest(name = "{0} -> {1}")
        @CsvSource({
                "BTCUSDT,  BTC-USD",
                "ETHUSDT,  ETH-USD",
                "BTCUSDC,  BTC-USD",
                "BTCBUSD,  BTC-USD",
                "BTCFDUSD, BTC-USD",
                "ETHBTC,   ETH-BTC",
                "BNBETH,   BNB-ETH",
                "BTCEUR,   BTC-EUR",
                "btcusdt,  BTC-USD",
                "  BTCUSDT  , BTC-USD"
        })
        void splitsOnLongestKnownQuoteAsset(String raw, String expected) {
            assertThat(normalizer.normalizeConcatenated(raw)).isEqualTo(expected);
        }

        /**
         * The reason the quote list is ordered longest-first. A naive scan that matched
         * "USD" before "USDT" would produce BTCU-SDT and silently invent an instrument.
         */
        @Test
        void prefersUsdtOverUsdWhenBothCouldMatch() {
            assertThat(normalizer.normalizeConcatenated("BTCUSDT")).isEqualTo("BTC-USD");
            assertThat(new SymbolNormalizer(Fixtures.gatewayProperties(false))
                    .normalizeConcatenated("BTCUSDT")).isEqualTo("BTC-USDT");
        }

        @Test
        void passesThroughUnknownQuoteAssetRatherThanGuessing() {
            // Better a slightly odd symbol on the topic than a confidently wrong split.
            assertThat(normalizer.normalizeConcatenated("FOOBAR")).isEqualTo("FOOBAR");
        }

        @Test
        void doesNotSplitWhenTheSymbolIsOnlyTheQuoteAsset() {
            assertThat(normalizer.normalizeConcatenated("USDT")).isEqualTo("USDT");
        }
    }

    @Nested
    @DisplayName("delimited venue symbols (Coinbase, Kraken)")
    class Delimited {

        @ParameterizedTest(name = "{0} -> {1}")
        @CsvSource({
                "BTC-USD,  BTC-USD",
                "BTC/USD,  BTC-USD",
                "BTC_USD,  BTC-USD",
                "ETH/USDT, ETH-USD",
                "btc/usd,  BTC-USD",
                "XBT/USD,  BTC-USD",
                "ETH/XBT,  ETH-BTC"
        })
        void normalizesAnyDelimiterAndAliasesXbtToBtc(String raw, String expected) {
            assertThat(normalizer.normalizeDelimited(raw)).isEqualTo(expected);
        }

        @Test
        void fallsBackToConcatenatedParsingWhenNoDelimiterPresent() {
            assertThat(normalizer.normalizeDelimited("BTCUSDT")).isEqualTo("BTC-USD");
        }

        @Test
        void fallsBackWhenDelimiterLeavesAnEmptySide() {
            assertThat(normalizer.normalizeDelimited("BTC/")).isEqualTo("BTC");
        }
    }

    @Nested
    @DisplayName("absent input")
    class Absent {

        /**
         * Returning null rather than throwing is deliberate: the canonical model owns the
         * "this field is mandatory" decision so every DLQ message reads the same way.
         */
        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {"   "})
        void returnsNullForBlankInput(String raw) {
            assertThat(normalizer.normalizeConcatenated(raw)).isNull();
            assertThat(normalizer.normalizeDelimited(raw)).isNull();
        }
    }

    @Nested
    @DisplayName("stablecoin collapsing toggle")
    class StablecoinCollapsing {

        private final SymbolNormalizer distinct = new SymbolNormalizer(Fixtures.gatewayProperties(false));

        @ParameterizedTest(name = "{0} stays {1} when collapsing is off")
        @CsvSource({
                "BTC/USDT, BTC-USDT",
                "BTC-USDC, BTC-USDC",
                "BTCDAI,   BTC-DAI"
        })
        void keepsStablecoinsDistinctWhenDisabled(String raw, String expected) {
            assertThat(distinct.normalizeDelimited(raw)).isEqualTo(expected);
        }

        @Test
        void neverRewritesTheBaseAsset() {
            // USDT as a *base* is a different instrument; only the quote side collapses.
            assertThat(normalizer.normalizeDelimited("USDT/TRY")).isEqualTo("USDT-TRY");
        }
    }
}
