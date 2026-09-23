package com.mdg.gateway.client;

import com.mdg.gateway.config.ExchangeProperties;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.service.MarketDataIngestionService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Shared WebSocket machinery for every venue: connect, subscribe, reassemble frames,
 * detect death, reconnect with backoff.
 *
 * <h2>Why {@code java.net.http.WebSocket} and not spring-websocket</h2>
 * This gateway only ever dials <em>out</em>. The JDK client is the whole outbound stack in
 * one class with no servlet container underneath, its {@code Listener} is demand-driven
 * (see below), and it answers venue Pings with Pongs automatically.
 *
 * <h2>The concurrency design — the part worth reading twice</h2>
 * {@code Listener.onText} returns a {@link CompletionStage}, and the JDK guarantees it will
 * not invoke the listener again until that stage completes. This class returns a stage that
 * runs the whole normalize-and-publish pipeline on a <b>virtual thread</b>, then requests
 * the next frame once it finishes. Three properties fall out of that:
 *
 * <ul>
 *   <li><b>No OS thread ever blocks.</b> The pipeline parks on the Kafka broker ack; a
 *       virtual thread unmounts from its carrier while parked, so the {@code HttpClient}'s
 *       selector threads stay free.</li>
 *   <li><b>Frames stay ordered.</b> One frame is in flight per connection, so trades for a
 *       symbol reach the topic in the order the venue sent them. Dispatching fire-and-forget
 *       would be faster and would reorder them — for market data that is the wrong trade.</li>
 *   <li><b>Backpressure is real.</b> If the broker slows down, we stop requesting frames and
 *       TCP backpressure propagates to the venue, rather than the heap absorbing the burst.</li>
 * </ul>
 *
 * The ceiling this imposes is one frame per round-trip per venue (~200–1000 frames/s at
 * typical ack latencies), which is comfortably above a single-symbol feed. If you subscribe
 * to hundreds of symbols on one socket, shard across sockets before you relax the ordering.
 *
 * <h2>Liveness</h2>
 * A TCP connection can stay open and silent after a venue-side fault — no close frame, no
 * error, just nothing. Automatic Pong replies do not detect that. The idle watchdog is the
 * only thing that does: no frame within {@code idleTimeout} forces a reconnect.
 */
public abstract class AbstractExchangeWebSocketClient implements ExchangeWebSocketClient {

    private static final Logger log = LoggerFactory.getLogger(AbstractExchangeWebSocketClient.class);

    private final Exchange exchange;
    private final ExchangeProperties.Connection config;
    private final MarketDataIngestionService ingestionService;
    private final ExecutorService ingestionExecutor;
    private final ScheduledExecutorService scheduler;
    private final HttpClient httpClient;
    private final BackoffPolicy backoffPolicy;

    private final Counter connectCounter;
    private final Counter disconnectCounter;
    private final Counter oversizedFrameCounter;

    private final AtomicReference<WebSocket> socket = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicLong lastFrameNanos = new AtomicLong(System.nanoTime());

    private volatile ScheduledFuture<?> watchdog;

    protected AbstractExchangeWebSocketClient(Exchange exchange,
                                              ExchangeProperties.Connection config,
                                              MarketDataIngestionService ingestionService,
                                              ExecutorService ingestionExecutor,
                                              ScheduledExecutorService scheduler,
                                              HttpClient httpClient,
                                              MeterRegistry meterRegistry) {
        this.exchange = exchange;
        this.config = config;
        this.ingestionService = ingestionService;
        this.ingestionExecutor = ingestionExecutor;
        this.scheduler = scheduler;
        this.httpClient = httpClient;
        this.backoffPolicy = new BackoffPolicy(config.backoff());

        Tags tags = Tags.of("exchange", exchange.name());
        this.connectCounter = Counter.builder("mdg.ws.connected")
                .tags(tags).description("Successful WebSocket handshakes").register(meterRegistry);
        this.disconnectCounter = Counter.builder("mdg.ws.disconnected")
                .tags(tags).description("Connection losses of any cause").register(meterRegistry);
        this.oversizedFrameCounter = Counter.builder("mdg.ws.frames.oversized")
                .tags(tags).description("Fragmented frames exceeding maxFrameBytes").register(meterRegistry);
    }

    /**
     * Venue-specific subscription frames, sent immediately after the handshake.
     * Return an empty list for venues that encode the subscription in the URL (Binance).
     */
    protected abstract List<String> subscriptionMessages();

    @Override
    public Exchange exchange() {
        return exchange;
    }

    @Override
    public boolean isConnected() {
        WebSocket current = socket.get();
        return current != null && !current.isInputClosed() && !current.isOutputClosed();
    }

    @Override
    public int consecutiveFailures() {
        return consecutiveFailures.get();
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        log.info("Starting {} feed: {} symbols={}", exchange, config.uri(), config.symbols());
        armWatchdog();
        connect();
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("Stopping {} feed", exchange);

        ScheduledFuture<?> currentWatchdog = watchdog;
        if (currentWatchdog != null) {
            currentWatchdog.cancel(false);
        }

        WebSocket current = socket.getAndSet(null);
        if (current == null) {
            return;
        }
        try {
            // Best-effort graceful close; abort() guarantees the socket is released even if
            // the venue never answers the close handshake.
            current.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown")
                    .orTimeout(2, TimeUnit.SECONDS)
                    .exceptionally(ex -> null)
                    .join();
        } catch (RuntimeException ex) {
            log.debug("{} close handshake did not complete cleanly", exchange, ex);
        } finally {
            current.abort();
        }
    }

    // ------------------------------------------------------------------
    // Connect / reconnect
    // ------------------------------------------------------------------

    private void connect() {
        if (!running.get()) {
            return;
        }
        log.debug("Dialling {} at {}", exchange, config.uri());

        httpClient.newWebSocketBuilder()
                .connectTimeout(config.connectTimeout())
                .buildAsync(config.uri(), new FrameListener())
                .whenComplete((webSocket, error) -> {
                    if (error != null) {
                        int attempt = consecutiveFailures.incrementAndGet();
                        log.warn("{} connect attempt {} failed: {}", exchange, attempt, rootMessage(error));
                        scheduleReconnect();
                        return;
                    }
                    onConnected(webSocket);
                });
    }

    private void onConnected(WebSocket webSocket) {
        socket.set(webSocket);
        consecutiveFailures.set(0);
        reconnectScheduled.set(false);
        touch();
        connectCounter.increment();
        log.info("{} connected", exchange);

        // Subscriptions are sent sequentially: several venues reject a burst of concurrent
        // subscribe frames on a fresh socket.
        CompletableFuture<WebSocket> chain = CompletableFuture.completedFuture(webSocket);
        for (String message : subscriptionMessages()) {
            chain = chain.thenCompose(ws -> ws.sendText(message, true));
        }
        chain.whenComplete((ws, error) -> {
            if (error != null) {
                log.error("{} subscription failed; forcing reconnect", exchange, error);
                forceReconnect("subscription failure");
            } else if (!subscriptionMessages().isEmpty()) {
                log.info("{} subscribed: {}", exchange, config.symbols());
            }
        });
    }

    private void scheduleReconnect() {
        if (!running.get()) {
            return;
        }
        // onError and onClose both fire for the same failure on some venues; only the first
        // one through may schedule, or the backoff would be defeated by a duplicate attempt.
        if (!reconnectScheduled.compareAndSet(false, true)) {
            return;
        }
        socket.set(null);

        Duration delay = backoffPolicy.delayForAttempt(consecutiveFailures.get() + 1);
        log.info("{} reconnecting in {} ms (consecutive failures: {})",
                exchange, delay.toMillis(), consecutiveFailures.get());

        scheduler.schedule(() -> {
            reconnectScheduled.set(false);
            connect();
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void forceReconnect(String reason) {
        WebSocket current = socket.getAndSet(null);
        if (current != null) {
            current.abort();
        }
        disconnectCounter.increment();
        consecutiveFailures.incrementAndGet();
        log.warn("{} forcing reconnect: {}", exchange, reason);
        scheduleReconnect();
    }

    // ------------------------------------------------------------------
    // Liveness
    // ------------------------------------------------------------------

    private void armWatchdog() {
        long periodMillis = Math.max(1_000, config.idleTimeout().toMillis() / 2);
        watchdog = scheduler.scheduleAtFixedRate(this::checkLiveness,
                periodMillis, periodMillis, TimeUnit.MILLISECONDS);
    }

    private void checkLiveness() {
        if (!running.get() || !isConnected()) {
            return;
        }
        long idleNanos = System.nanoTime() - lastFrameNanos.get();
        if (idleNanos > config.idleTimeout().toNanos()) {
            forceReconnect("no frames for " + TimeUnit.NANOSECONDS.toSeconds(idleNanos) + "s");
        }
    }

    private void touch() {
        lastFrameNanos.set(System.nanoTime());
    }

    // ------------------------------------------------------------------

    /**
     * Per-connection listener. A fresh instance is created for every connect attempt, so the
     * fragment buffer can be a plain field: the JDK invokes listener methods for one
     * connection strictly sequentially.
     */
    private final class FrameListener implements WebSocket.Listener {

        private final StringBuilder fragments = new StringBuilder();

        @Override
        public void onOpen(WebSocket webSocket) {
            // The JDK starts every connection with zero demand; without this first request
            // no frame is ever delivered.
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            touch();

            if (fragments.length() + data.length() > config.maxFrameBytes()) {
                // A peer streaming an unbounded fragmented message would otherwise grow the
                // heap until the JVM dies. Drop the partial message and restart the socket.
                oversizedFrameCounter.increment();
                fragments.setLength(0);
                forceReconnect("fragmented frame exceeded maxFrameBytes=" + config.maxFrameBytes());
                return null;
            }

            fragments.append(data);
            if (!last) {
                webSocket.request(1);
                return null;
            }

            String frame = fragments.toString();
            fragments.setLength(0);

            // Returning this stage is what keeps frames ordered: the JDK will not deliver
            // the next one until the pipeline has finished with this one.
            return CompletableFuture
                    .runAsync(() -> ingestionService.ingest(exchange, frame), ingestionExecutor)
                    .whenComplete((ignored, error) -> {
                        if (error != null) {
                            // ingest() is total, so this is a bug rather than a bad frame —
                            // log it and keep the feed alive regardless.
                            log.error("{} ingest task failed unexpectedly", exchange, error);
                        }
                        if (running.get()) {
                            webSocket.request(1);
                        }
                    });
        }

        @Override
        public CompletionStage<?> onBinary(WebSocket webSocket, java.nio.ByteBuffer data, boolean last) {
            // None of the three venues use binary frames on these channels. Consume and
            // ignore rather than stalling demand if one starts.
            touch();
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            disconnectCounter.increment();
            consecutiveFailures.incrementAndGet();
            log.warn("{} closed by peer: {} {}", exchange, statusCode, reason);
            scheduleReconnect();
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            disconnectCounter.increment();
            consecutiveFailures.incrementAndGet();
            log.error("{} socket error: {}", exchange, rootMessage(error));
            scheduleReconnect();
        }
    }

    private static String rootMessage(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }
}
