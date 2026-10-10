package com.mdg.gateway.client;

import com.mdg.gateway.config.ExchangeProperties;
import com.mdg.gateway.config.VirtualThreadConfig;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.service.MarketDataIngestionService;
import com.mdg.gateway.venue.FeedRuntime;
import com.mdg.gateway.venue.Venue;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * Owns the lifecycle of every enabled venue feed.
 *
 * <p>{@link SmartLifecycle} rather than {@code @PostConstruct}: sockets must not open until
 * the Kafka producer and the rest of the context are fully initialized, or the first ticks
 * arrive with nowhere to go. A late {@code phase} guarantees this starts last and stops
 * first.
 *
 * <p>Clients are constructed here rather than declared as beans so that a disabled venue
 * costs nothing — no bean, no socket, no reconnect timer. That is what lets the test
 * profile guarantee CI never dials a public exchange.
 */
@Component
public class ExchangeConnectionManager implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ExchangeConnectionManager.class);

    private final ExchangeProperties exchangeProperties;
    private final Map<Exchange, Venue> venues = new EnumMap<>(Exchange.class);
    private final FeedRuntime runtime;
    private final CircuitBreakerRegistry circuitBreakerRegistry;

    private final List<ExchangeWebSocketClient> clients = new ArrayList<>();
    private volatile boolean running;

    public ExchangeConnectionManager(
            ExchangeProperties exchangeProperties,
            List<Venue> venues,
            MarketDataIngestionService ingestionService,
            @Qualifier(VirtualThreadConfig.INGESTION_EXECUTOR) ExecutorService ingestionExecutor,
            @Qualifier(VirtualThreadConfig.RECONNECT_SCHEDULER) ScheduledExecutorService scheduler,
            HttpClient marketDataHttpClient,
            CircuitBreakerRegistry circuitBreakerRegistry,
            MeterRegistry meterRegistry) {
        this.exchangeProperties = exchangeProperties;
        venues.forEach(venue -> this.venues.put(venue.exchange(), venue));
        this.runtime = new FeedRuntime(ingestionService, ingestionExecutor, scheduler, marketDataHttpClient, meterRegistry);
        this.circuitBreakerRegistry = circuitBreakerRegistry;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        exchangeProperties.exchanges().forEach((exchange, config) -> {
            if (!config.enabled()) {
                log.info("{} feed disabled by configuration", exchange);
                return;
            }
            ExchangeWebSocketClient client = create(exchange, config);
            clients.add(client);
            client.start();
        });

        running = true;
        if (clients.isEmpty()) {
            log.info("No exchange feeds enabled — gateway is running in DLQ-replay-only mode");
        } else {
            log.info("Started {} exchange feed(s): {}", clients.size(),
                    clients.stream().map(c -> c.exchange().name()).toList());
        }
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        // Close every client even if one throws, or a single misbehaving venue would leak
        // the rest of the sockets on shutdown.
        for (ExchangeWebSocketClient client : clients) {
            try {
                client.close();
            } catch (RuntimeException ex) {
                log.warn("Error closing {} feed", client.exchange(), ex);
            }
        }
        clients.clear();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Start last, stop first: the pipeline must outlive the sockets that feed it. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1000;
    }

    public static String connectionBreakerName(Exchange exchange) {
        return "venue-" + exchange.name().toLowerCase();
    }

    public Map<Exchange, String> connectionCircuitStates() {
        return clients.stream().collect(Collectors.toMap(
                ExchangeWebSocketClient::exchange,
                ExchangeWebSocketClient::connectionCircuitState,
                (a, b) -> a));
    }

    /** Live connection state per venue, surfaced by {@code ExchangeHealthIndicator}. */
    public Map<Exchange, Boolean> connectionStatus() {
        return clients.stream().collect(Collectors.toMap(
                ExchangeWebSocketClient::exchange,
                ExchangeWebSocketClient::isConnected,
                (a, b) -> a));
    }

    public Map<Exchange, Integer> consecutiveFailures() {
        return clients.stream().collect(Collectors.toMap(
                ExchangeWebSocketClient::exchange,
                ExchangeWebSocketClient::consecutiveFailures,
                (a, b) -> a));
    }

    private ExchangeWebSocketClient create(Exchange exchange, ExchangeProperties.Connection config) {
        // From the registry, so each venue's breaker is configured in application.yml
        // (resilience4j.circuitbreaker.instances.venue-<name>) and shows up in
        // /actuator/circuitbreakers and the resilience4j metrics next to the Kafka one.
        CircuitBreaker breaker = circuitBreakerRegistry.circuitBreaker(connectionBreakerName(exchange));
        Venue venue = venues.get(exchange);
        if (venue == null) {
            throw new IllegalStateException("gateway.exchanges." + exchange.name().toLowerCase()
                    + " is enabled but no Venue is registered for it");
        }
        return venue.createClient(config, breaker, runtime);
    }
}
