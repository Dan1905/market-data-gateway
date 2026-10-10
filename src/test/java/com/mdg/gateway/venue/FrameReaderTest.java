package com.mdg.gateway.venue;

import com.mdg.gateway.config.JacksonConfig;
import com.mdg.gateway.exception.PayloadParsingException;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.support.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FrameReaderTest {

    private final FrameReader frames = new FrameReader(JacksonConfig.marketDataObjectMapper());

    private static CanonicalTradeEvent trade(String venueSymbol, String id) {
        return Fixtures.canonicalEvent().toBuilder().venueSymbol(venueSymbol).tradeId(id)
                .eventId(CanonicalTradeEvent.deterministicEventId("BINANCE", venueSymbol, id)).build();
    }

    @Test
    @DisplayName("trade id comparison is numeric for digit strings, lexical otherwise")
    void tradeIdComparison() {
        assertThat(FrameReader.compareTradeIds("99", "100")).isNegative();
        assertThat(FrameReader.compareTradeIds("100", "100")).isZero();
        assertThat(FrameReader.compareTradeIds("b", "a")).isPositive();
    }

    @Test
    @DisplayName("a batch is put into ascending trade-id order within each venue symbol")
    void ordersBatches() {
        List<CanonicalTradeEvent> ordered = FrameReader.inTradeOrder(List.of(
                trade("ETHUSDT", "7"), trade("BTCUSDT", "1000"), trade("BTCUSDT", "999"), trade("ETHUSDT", "6")));

        assertThat(ordered).extracting(e -> e.venueSymbol() + ":" + e.tradeId())
                .containsExactly("BTCUSDT:999", "BTCUSDT:1000", "ETHUSDT:6", "ETHUSDT:7");
    }

    @Test
    @DisplayName("malformed JSON and a violated invariant both become the pipeline's one parse exception")
    void failuresBecomeParseExceptions() {
        assertThatThrownBy(() -> frames.read(Exchange.KRAKEN, "{ not json", java.util.Map.class))
                .isInstanceOf(PayloadParsingException.class)
                .hasMessageContaining("Malformed KRAKEN JSON");
        assertThatThrownBy(() -> frames.read(Exchange.KRAKEN, "null", java.util.Map.class))
                .isInstanceOf(PayloadParsingException.class);
        assertThatThrownBy(() -> frames.map(Exchange.BINANCE, "{}", () -> {
            throw new IllegalArgumentException("price must be positive");
        })).isInstanceOf(PayloadParsingException.class).hasMessageContaining("Invalid BINANCE payload");
    }
}
