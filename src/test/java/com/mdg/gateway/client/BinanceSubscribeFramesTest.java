package com.mdg.gateway.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinanceSubscribeFramesTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private static List<String> symbols(int n) {
        return IntStream.range(0, n).mapToObj(i -> "SYM" + i + "USDT").toList();
    }

    @Test
    @DisplayName("packs streams into as few frames as possible, each with its own id")
    void chunksStreams() throws Exception {
        List<String> frames = BinanceWebSocketClient.buildSubscribeFrames(symbols(450), mapper);

        // Every control frame counts against Binance's 5 messages/second limit.
        assertThat(frames).hasSize(3);
        int total = 0;
        for (int i = 0; i < frames.size(); i++) {
            var node = mapper.readTree(frames.get(i));
            assertThat(node.get("method").asText()).isEqualTo("SUBSCRIBE");
            assertThat(node.get("id").asInt()).isEqualTo(i + 1);
            total += node.get("params").size();
        }
        assertThat(total).isEqualTo(450);
        assertThat(frames.get(0)).contains("\"sym0usdt@trade\"");
    }

    @Test
    @DisplayName("normalizes case and drops duplicate symbols")
    void normalizesAndDedupes() {
        assertThat(BinanceWebSocketClient.buildSubscribeFrames(List.of("BTCUSDT", " btcusdt ", "ETHUSDT"), mapper))
                .containsExactly("{\"method\":\"SUBSCRIBE\",\"params\":[\"btcusdt@trade\",\"ethusdt@trade\"],\"id\":1}");
    }

    @Test
    @DisplayName("refuses more streams than Binance allows on one connection")
    void rejectsTooManyStreams() {
        assertThatThrownBy(() -> BinanceWebSocketClient.buildSubscribeFrames(symbols(1025), mapper))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1024");
    }
}
