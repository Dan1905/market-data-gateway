package com.mdg.gateway.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.config.ExchangeProperties;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.service.MarketDataIngestionService;
import io.micrometer.core.instrument.MeterRegistry;

import java.net.http.HttpClient;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Kraken WebSocket v2 trade feed.
 *
 * <p>Subscription frame:
 * <pre>{@code {"method":"subscribe","params":{"channel":"trade","symbol":["BTC/USD"],"snapshot":false}}}</pre>
 *
 * <p>{@code snapshot:false} is deliberate. On subscribe Kraken otherwise replays a batch of
 * recent trades with their original timestamps; republishing those as fresh canonical
 * events would inject minutes-old prices into the topic every time the gateway reconnects,
 * and a reconnect loop would emit them repeatedly. Live updates only.
 *
 * <p>Kraken v2 also emits {@code status} and {@code heartbeat} frames on the same socket;
 * {@code PayloadTransformationService} filters them out by channel so they never reach the
 * DLQ.
 */
public class KrakenWebSocketClient extends AbstractExchangeWebSocketClient {

    private static final String CHANNEL_TRADE = "trade";

    private final ExchangeProperties.Connection config;
    private final ObjectMapper objectMapper;

    public KrakenWebSocketClient(ExchangeProperties.Connection config,
                                 MarketDataIngestionService ingestionService,
                                 ExecutorService ingestionExecutor,
                                 ScheduledExecutorService scheduler,
                                 HttpClient httpClient,
                                 ObjectMapper objectMapper,
                                 MeterRegistry meterRegistry) {
        super(Exchange.KRAKEN, config, ingestionService, ingestionExecutor, scheduler,
                httpClient, meterRegistry);
        this.config = config;
        this.objectMapper = objectMapper;
    }

    @Override
    protected List<String> subscriptionMessages() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("channel", CHANNEL_TRADE);
        params.put("symbol", config.symbols());
        params.put("snapshot", false);

        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("method", "subscribe");
        frame.put("params", params);

        try {
            return List.of(objectMapper.writeValueAsString(frame));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Could not build Kraken subscribe frame", ex);
        }
    }
}
