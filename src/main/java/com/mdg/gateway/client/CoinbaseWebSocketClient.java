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
 * Coinbase Advanced Trade ticker feed.
 *
 * <p>Subscription frame:
 * <pre>{@code {"type":"subscribe","product_ids":["BTC-USD"],"channel":"ticker"}}</pre>
 *
 * <p>One frame per channel — Coinbase rejects a subscribe that names several channels at
 * once, so the base class's sequential send is what makes adding channels here safe.
 *
 * <p><b>Operational note:</b> Coinbase has been progressively moving Advanced Trade
 * channels behind JWT authentication. If the socket connects and then closes immediately
 * with an authentication error, this venue needs a signed JWT in the subscribe frame — that
 * requires an API key and is out of scope for a public-data gateway. Disable the venue with
 * {@code gateway.exchanges.coinbase.enabled=false} rather than letting it reconnect-loop.
 */
public class CoinbaseWebSocketClient extends AbstractExchangeWebSocketClient {

    private static final String CHANNEL_TICKER = "ticker";
    private static final String CHANNEL_HEARTBEATS = "heartbeats";

    private final ExchangeProperties.Connection config;
    private final ObjectMapper objectMapper;

    public CoinbaseWebSocketClient(ExchangeProperties.Connection config,
                                   MarketDataIngestionService ingestionService,
                                   ExecutorService ingestionExecutor,
                                   ScheduledExecutorService scheduler,
                                   HttpClient httpClient,
                                   ObjectMapper objectMapper,
                                   MeterRegistry meterRegistry) {
        super(Exchange.COINBASE, config, ingestionService, ingestionExecutor, scheduler,
                httpClient, meterRegistry);
        this.config = config;
        this.objectMapper = objectMapper;
    }

    @Override
    protected List<String> subscriptionMessages() {
        // Subscribing to heartbeats keeps the socket producing frames during quiet periods,
        // which stops the idle watchdog from cycling a perfectly healthy connection.
        return List.of(
                subscribeFrame(CHANNEL_HEARTBEATS),
                subscribeFrame(CHANNEL_TICKER));
    }

    private String subscribeFrame(String channel) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("type", "subscribe");
        frame.put("product_ids", config.symbols());
        frame.put("channel", channel);
        try {
            return objectMapper.writeValueAsString(frame);
        } catch (JsonProcessingException ex) {
            // Serializing a literal map cannot realistically fail; if it does, the config is
            // unusable and failing loudly at startup beats a silently unsubscribed feed.
            throw new IllegalStateException("Could not build Coinbase subscribe frame", ex);
        }
    }
}
