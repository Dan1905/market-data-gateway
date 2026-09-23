package com.mdg.gateway.client;

import com.mdg.gateway.config.ExchangeProperties;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.service.MarketDataIngestionService;
import io.micrometer.core.instrument.MeterRegistry;

import java.net.http.HttpClient;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Binance public trade stream.
 *
 * <p>Binance is the odd one out: the subscription is encoded in the <em>path</em>
 * ({@code /ws/btcusdt@trade}), so there is no subscribe frame to send after the handshake —
 * ticks start arriving immediately. Multiple symbols use the combined-stream form
 * ({@code /stream?streams=a@trade/b@trade}), which is why {@code uri} is configured whole
 * rather than assembled here from the symbol list.
 *
 * <p>Binance sends a Ping every ~3 minutes and disconnects a client that does not Pong
 * within 10 minutes. The JDK {@code HttpClient} answers Pings automatically, so there is
 * nothing to implement — but the 45s idle watchdog in the base class is still what catches
 * a socket that goes silent without closing.
 */
public class BinanceWebSocketClient extends AbstractExchangeWebSocketClient {

    public BinanceWebSocketClient(ExchangeProperties.Connection config,
                                  MarketDataIngestionService ingestionService,
                                  ExecutorService ingestionExecutor,
                                  ScheduledExecutorService scheduler,
                                  HttpClient httpClient,
                                  MeterRegistry meterRegistry) {
        super(Exchange.BINANCE, config, ingestionService, ingestionExecutor, scheduler,
                httpClient, meterRegistry);
    }

    @Override
    protected List<String> subscriptionMessages() {
        return List.of();
    }
}
