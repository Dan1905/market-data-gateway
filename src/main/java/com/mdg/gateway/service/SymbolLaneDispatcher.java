package com.mdg.gateway.service;

import com.mdg.gateway.config.GatewayProperties;
import com.mdg.gateway.config.VirtualThreadConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Runs work in parallel across symbols while keeping it strictly ordered within one symbol.
 *
 * <h2>Why lanes</h2>
 * The first version kept order by allowing one frame in flight per WebSocket connection: the
 * next frame was not read until the previous one had been published and acknowledged. That is
 * correct, but it serializes every symbol on a connection behind the slowest broker ack, and it
 * sends to Kafka one record at a time so the producer can never batch. With one symbol per
 * venue that was fine. With dozens of symbols on one Binance connection it is the bottleneck.
 *
 * <p>Ordering only matters <em>within</em> a venue symbol — trade 1001 on BTCUSDT must precede
 * 1002 on BTCUSDT, but has no ordering relationship with trade 55 on ETHUSDT. So each venue
 * symbol gets its own lane: a FIFO chain of tasks, each running on a virtual thread only after
 * the previous one in the same lane has finished. Lanes run concurrently with each other.
 *
 * <h2>Backpressure is preserved</h2>
 * {@link #submit} takes a permit from a global in-flight budget and blocks when it is
 * exhausted. The caller is the WebSocket frame handler, running on a virtual thread, so
 * blocking is cheap — and while it blocks the socket's next frame is not requested, so TCP
 * backpressure still reaches the venue when the broker falls behind. Lanes add concurrency
 * without removing the brake.
 *
 * <p>Lanes are keyed by venue symbol, which is bounded by the subscription list, so the map
 * does not grow without limit.
 */
@Component
public class SymbolLaneDispatcher implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(SymbolLaneDispatcher.class);

    private final Executor executor;
    private final Semaphore inFlight;
    private final int maxInFlight;
    private final Duration drainTimeout;
    private final Map<String, Lane> lanes = new ConcurrentHashMap<>();
    private final Counter backpressureWaits;
    private volatile boolean running;

    @Autowired
    public SymbolLaneDispatcher(@Qualifier(VirtualThreadConfig.INGESTION_EXECUTOR) Executor executor,
                                GatewayProperties properties,
                                MeterRegistry meterRegistry) {
        this(executor, properties.lanes().maxInFlight(), properties.lanes().drainTimeout(), meterRegistry);
    }

    SymbolLaneDispatcher(Executor executor, int maxInFlight, Duration drainTimeout, MeterRegistry meterRegistry) {
        this.executor = executor;
        this.maxInFlight = maxInFlight;
        this.inFlight = new Semaphore(maxInFlight);
        this.drainTimeout = drainTimeout;
        this.backpressureWaits = Counter.builder("mdg.lanes.backpressure.waits")
                .description("Submissions that had to wait because the in-flight budget was exhausted")
                .register(meterRegistry);
        Gauge.builder("mdg.lanes.active", lanes, Map::size)
                .description("Distinct venue-symbol lanes seen").register(meterRegistry);
        Gauge.builder("mdg.lanes.in.flight", this, SymbolLaneDispatcher::inFlightCount)
                .description("Tasks queued or running across all lanes").register(meterRegistry);
    }

    /**
     * Queues a task behind every earlier task with the same key.
     *
     * <p>Blocks while the global in-flight budget is exhausted. That is deliberate — see the
     * class comment on backpressure.
     *
     * @return a future completing when this task has run. A task that throws completes the
     *         future exceptionally but does not stall its lane.
     */
    public CompletableFuture<Void> submit(String laneKey, Runnable task) {
        acquirePermit();
        CompletableFuture<Void> done = lanes.computeIfAbsent(laneKey, key -> new Lane()).append(task, executor);
        // Released on completion rather than inside the task: if the executor rejects the task
        // (it does during shutdown), the future still completes — exceptionally — and the permit
        // comes back. Releasing inside the task would leak it, because the task never runs.
        done.whenComplete((ignored, error) -> inFlight.release());
        return done;
    }

    public int inFlightCount() {
        return maxInFlight - inFlight.availablePermits();
    }

    private void acquirePermit() {
        if (inFlight.tryAcquire()) {
            return;
        }
        backpressureWaits.increment();
        try {
            inFlight.acquire();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted waiting for lane capacity", ex);
        }
    }

    // ------------------------------------------------------------------
    // Lifecycle: after the sockets stop, let queued work finish before Kafka shuts down.
    // ------------------------------------------------------------------

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        running = false;
        long deadline = System.nanoTime() + drainTimeout.toNanos();
        while (inFlightCount() > 0 && System.nanoTime() < deadline) {
            try {
                TimeUnit.MILLISECONDS.sleep(20);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        int left = inFlightCount();
        if (left > 0) {
            log.warn("Shutting down with {} lane task(s) still in flight after {}", left, drainTimeout);
        } else {
            log.info("All lanes drained");
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * Stop after the WebSocket clients (which stop first, at MAX_VALUE - 1000) and before the
     * Kafka producer, so no new work arrives and queued work can still publish.
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 2000;
    }

    /** A FIFO chain of tasks: each starts only after the previous one finished. */
    private static final class Lane {

        private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);

        synchronized CompletableFuture<Void> append(Runnable task, Executor executor) {
            // handle() swallows the previous task's failure so one bad task cannot stall the
            // lane; the failure is still visible on that task's own future.
            CompletableFuture<Void> next = tail.handle((ignored, error) -> null)
                    .thenRunAsync(task, executor);
            tail = next;
            return next;
        }
    }
}
