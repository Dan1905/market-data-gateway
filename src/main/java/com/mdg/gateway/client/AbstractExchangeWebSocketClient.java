package com.mdg.gateway.client;

import com.mdg.gateway.config.ExchangeProperties;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.service.MarketDataIngestionService;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
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
 * parses the frame on a <b>virtual thread</b> and hands each event to its symbol's lane
 * (see {@code SymbolLaneDispatcher}), then requests the next frame. Three properties fall out:
 *
 * <ul>
 *   <li><b>No OS thread ever blocks.</b> Anything that waits — the rate limiter, the lanes'
 *       in-flight budget, a broker ack — parks a virtual thread, which unmounts from its
 *       carrier, so the {@code HttpClient}'s selector threads stay free.</li>
 *   <li><b>Frames are handed over in order.</b> One frame is parsed at a time per connection,
 *       so each symbol's events enter its lane in the order the venue sent them.</li>
 *   <li><b>Backpressure is real.</b> When the lanes' budget is spent the hand-off blocks, the
 *       next frame is not requested, and TCP backpressure reaches the venue instead of the
 *       heap absorbing the burst.</li>
 * </ul>
 *
 * <h2>Staying inside the venue's limits</h2>
 * Reconnecting is where a client gets itself banned. {@link VenueConnectionGuard} decides
 * whether a dial is allowed at all (a per-venue circuit breaker, plus a cooldown when the
 * venue signals a rate limit), and everything this class sends goes through a per-connection
 * rate limiter.
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
    private final VenueConnectionGuard guard;
    private final RateLimiter outboundLimiter;

    private final Counter connectCounter;
    private final Counter disconnectCounter;
    private final Counter oversizedFrameCounter;

    private final AtomicReference<WebSocket> socket = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicLong lastFrameNanos = new AtomicLong(System.nanoTime());

    /** The connection attempt whose outcome the circuit breaker has not been told yet. */
    private final AtomicReference<VenueConnectionGuard.Attempt> pendingAttempt = new AtomicReference<>();

    private volatile ScheduledFuture<?> watchdog;

    protected AbstractExchangeWebSocketClient(Exchange exchange,
                                              ExchangeProperties.Connection config,
                                              MarketDataIngestionService ingestionService,
                                              ExecutorService ingestionExecutor,
                                              ScheduledExecutorService scheduler,
                                              HttpClient httpClient,
                                              CircuitBreaker connectionBreaker,
                                              MeterRegistry meterRegistry) {
        this.exchange = exchange;
        this.config = config;
        this.ingestionService = ingestionService;
        this.ingestionExecutor = ingestionExecutor;
        this.scheduler = scheduler;
        this.httpClient = httpClient;
        this.backoffPolicy = new BackoffPolicy(config.backoff());
        this.guard = new VenueConnectionGuard(exchange, config.limits(), connectionBreaker, meterRegistry);
        // Shared across reconnects on purpose: a reconnect must not earn a fresh allowance.
        this.outboundLimiter = RateLimiter.of("venue-outbound-" + exchange.name().toLowerCase(),
                RateLimiterConfig.custom()
                        .limitForPeriod(config.limits().outboundMessagesPerSecond())
                        .limitRefreshPeriod(Duration.ofSeconds(1))
                        .timeoutDuration(Duration.ofSeconds(30))
                        .build());

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

    @Override
    public String connectionCircuitState() {
        return guard.circuitState();
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

        // Our own shutdown is neither a success nor a failure of the venue.
        VenueConnectionGuard.Attempt attempt = pendingAttempt.getAndSet(null);
        if (attempt != null) {
            attempt.abandoned();
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
        // The venue told us to stay away: wait it out without touching the network.
        Duration cooldown = guard.cooldownRemaining();
        if (!cooldown.isZero()) {
            scheduleConnect(cooldown);
            return;
        }
        VenueConnectionGuard.Attempt attempt = guard.tryBegin().orElse(null);
        if (attempt == null) {
            // Breaker open. Ask again later; it lets one probe through once its wait is over.
            log.debug("{} connection circuit is {}; not dialling", exchange, guard.circuitState());
            scheduleConnect(config.backoff().maxDelay());
            return;
        }
        pendingAttempt.set(attempt);
        log.debug("Dialling {} at {}", exchange, config.uri());

        httpClient.newWebSocketBuilder()
                .connectTimeout(config.connectTimeout())
                .buildAsync(config.uri(), new FrameListener())
                .whenComplete((webSocket, error) -> {
                    if (error != null) {
                        int failures = consecutiveFailures.incrementAndGet();
                        log.warn("{} connect attempt failed ({} in a row): {}", exchange, failures, rootMessage(error));
                        failPendingAttempt(error);
                        VenueConnectionGuard.rateLimitSignal(error).ifPresent(signal ->
                                guard.rateLimited(signal.retryAfter(), "HTTP " + signal.httpStatus() + " on handshake"));
                        scheduleReconnect();
                        return;
                    }
                    onConnected(webSocket, attempt);
                });
    }

    private void onConnected(WebSocket webSocket, VenueConnectionGuard.Attempt attempt) {
        socket.set(webSocket);
        reconnectScheduled.set(false);
        touch();
        connectCounter.increment();
        log.info("{} connected", exchange);

        // An accepted handshake proves little: a throttled venue accepts and then hangs up.
        // Only a socket that is still ours after stableAfter resets the backoff and counts
        // as a success on the circuit breaker.
        scheduler.schedule(() -> {
            if (socket.get() == webSocket && attempt.stable()) {
                pendingAttempt.compareAndSet(attempt, null);
                consecutiveFailures.set(0);
                log.debug("{} connection is stable", exchange);
            }
        }, config.limits().stableAfter().toMillis(), TimeUnit.MILLISECONDS);

        List<String> subscriptions = subscriptionMessages();
        if (subscriptions.isEmpty()) {
            return;
        }
        // Sent one at a time (several venues reject concurrent subscribes on a fresh socket)
        // and through the outbound limiter. On a virtual thread, because waiting for a permit
        // blocks.
        CompletableFuture.runAsync(() -> subscriptions.forEach(message -> send(webSocket, message)), ingestionExecutor)
                .whenComplete((ignored, error) -> {
                    if (error != null) {
                        if (socket.get() == webSocket) {
                            log.error("{} subscription failed; forcing reconnect", exchange, error);
                            forceReconnect("subscription failure");
                        }
                    } else {
                        log.info("{} subscribed: {}", exchange, config.symbols());
                    }
                });
    }

    /** Sends one frame, waiting for the venue's outbound message allowance first. */
    private void send(WebSocket webSocket, String message) {
        RateLimiter.waitForPermission(outboundLimiter);
        webSocket.sendText(message, true).join();
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

        Duration delay = backoffPolicy.delayForAttempt(Math.max(1, consecutiveFailures.get()));
        Duration cooldown = guard.cooldownRemaining();
        if (cooldown.compareTo(delay) > 0) {
            delay = cooldown;
        }
        log.info("{} reconnecting in {} ms (consecutive failures: {}, circuit: {})",
                exchange, delay.toMillis(), consecutiveFailures.get(), guard.circuitState());
        scheduleConnect(delay);
    }

    private void scheduleConnect(Duration delay) {
        reconnectScheduled.set(true);
        scheduler.schedule(() -> {
            reconnectScheduled.set(false);
            connect();
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Tells the circuit breaker the current attempt failed, unless it already proved stable. */
    private void failPendingAttempt(Throwable cause) {
        VenueConnectionGuard.Attempt attempt = pendingAttempt.getAndSet(null);
        if (attempt != null) {
            attempt.failed(cause);
        }
    }

    private void forceReconnect(String reason) {
        WebSocket current = socket.getAndSet(null);
        if (current != null) {
            current.abort();
        }
        disconnectCounter.increment();
        consecutiveFailures.incrementAndGet();
        failPendingAttempt(new IllegalStateException(reason));
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
            failPendingAttempt(new IllegalStateException("closed by peer: " + statusCode));
            if (VenueConnectionGuard.isRateLimitClose(statusCode)) {
                guard.rateLimited(null, "close code " + statusCode);
            }
            scheduleReconnect();
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            disconnectCounter.increment();
            consecutiveFailures.incrementAndGet();
            log.error("{} socket error: {}", exchange, rootMessage(error));
            failPendingAttempt(error);
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
