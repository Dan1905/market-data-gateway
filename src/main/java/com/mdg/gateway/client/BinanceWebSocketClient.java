package com.mdg.gateway.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.config.ExchangeProperties;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.service.MarketDataIngestionService;
import io.micrometer.core.instrument.MeterRegistry;

import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Binance public trade stream, many symbols on one connection.
 *
 * <p>Connects to the bare {@code /ws} endpoint and subscribes with
 * <pre>{@code {"method":"SUBSCRIBE","params":["btcusdt@trade","ethusdt@trade"],"id":1}}</pre>
 * Binance acknowledges with {@code {"result":null,"id":1}} (skipped by the transformer) and
 * then streams raw trade frames for every listed symbol, each carrying its own {@code s}.
 *
 * <p>This replaces encoding the stream in the URL path: the symbol list now comes from
 * configuration alone, so adding a market is an env var change rather than a URI rewrite.
 *
 * <h2>Venue limits honoured here</h2>
 * <ul>
 *   <li>At most {@value #MAX_STREAMS_PER_CONNECTION} streams per connection — rejected at
 *       construction rather than discovered as a server-side close.</li>
 *   <li>Every JSON control message counts against the 5 incoming messages/second limit, so
 *       streams are packed {@value #STREAMS_PER_FRAME} to a frame: even the maximum stream
 *       count is only a handful of frames.</li>
 * </ul>
 *
 * <p>Binance pings every 20 s and drops a client that has not answered within a minute. The
 * JDK {@code HttpClient} answers pings automatically; the idle watchdog in the base class
 * still catches a socket that goes silent without closing.
 */
public class BinanceWebSocketClient extends AbstractExchangeWebSocketClient {

    static final int MAX_STREAMS_PER_CONNECTION = 1024;
    static final int STREAMS_PER_FRAME = 200;

    private final List<String> subscribeFrames;

    public BinanceWebSocketClient(ExchangeProperties.Connection config,
                                  MarketDataIngestionService ingestionService,
                                  ExecutorService ingestionExecutor,
                                  ScheduledExecutorService scheduler,
                                  HttpClient httpClient,
                                  ObjectMapper objectMapper,
                                  MeterRegistry meterRegistry) {
        super(Exchange.BINANCE, config, ingestionService, ingestionExecutor, scheduler,
                httpClient, meterRegistry);
        this.subscribeFrames = buildSubscribeFrames(config.symbols(), objectMapper);
    }

    @Override
    protected List<String> subscriptionMessages() {
        return subscribeFrames;
    }

    static List<String> buildSubscribeFrames(List<String> symbols, ObjectMapper objectMapper) {
        List<String> streams = symbols.stream()
                .map(symbol -> symbol.trim().toLowerCase(Locale.ROOT) + "@trade")
                .distinct()
                .toList();
        if (streams.size() > MAX_STREAMS_PER_CONNECTION) {
            throw new IllegalArgumentException("Binance allows at most " + MAX_STREAMS_PER_CONNECTION
                    + " streams per connection; " + streams.size() + " symbols configured");
        }
        List<String> frames = new ArrayList<>();
        for (int from = 0; from < streams.size(); from += STREAMS_PER_FRAME) {
            Map<String, Object> frame = new LinkedHashMap<>();
            frame.put("method", "SUBSCRIBE");
            frame.put("params", streams.subList(from, Math.min(from + STREAMS_PER_FRAME, streams.size())));
            frame.put("id", frames.size() + 1);
            try {
                frames.add(objectMapper.writeValueAsString(frame));
            } catch (JsonProcessingException ex) {
                throw new IllegalStateException("Could not build Binance subscribe frame", ex);
            }
        }
        return List.copyOf(frames);
    }
}
