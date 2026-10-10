package com.mdg.gateway.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.config.GatewayProperties;
import com.mdg.gateway.model.CanonicalTradeEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Holds events on local disk while the broker cannot take them, and publishes them in order
 * when it can.
 *
 * <h2>Why not the dead letter queue</h2>
 * The DLQ is for frames that are <em>wrong</em>. An event the broker refused is not wrong,
 * it is early — and the DLQ is a topic on the same broker that just refused it, so during a
 * broker outage a "dead-lettered" event was in fact dropped. A file on local disk has no
 * such dependency.
 *
 * <h2>How order survives an outage</h2>
 * Per-symbol order is the gateway's contract, and an outage must not break it. The rule is
 * simple: <b>while anything is waiting in the buffer, every new event goes behind it</b>
 * instead of to the broker ({@link #isActive}). One drainer thread publishes from the front.
 * Each symbol's lane is sequential, so a symbol's events enter the buffer in order, the
 * buffer is FIFO, and they leave it in order. Direct publishing only resumes once the buffer
 * is empty.
 *
 * <h2>Guarantee</h2>
 * At-least-once. A record leaves the buffer only after the broker acknowledged it; a crash
 * in between re-sends it on restart with the same deterministic {@code eventId}. The buffer
 * is bounded: once it is full {@link #offer} refuses, and the caller falls back to the DLQ.
 */
@Component
public class SpillBuffer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(SpillBuffer.class);

    private final GatewayProperties.Spill config;
    private final MarketDataProducer producer;
    private final ObjectMapper objectMapper;
    private final Counter spilledCounter;
    private final Counter drainedCounter;
    private final Counter rejectedCounter;
    private final Counter unreadableCounter;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition work = lock.newCondition();

    private volatile SpillJournal journal;
    private volatile boolean running;
    private Thread drainer;

    public SpillBuffer(GatewayProperties properties,
                       MarketDataProducer producer,
                       ObjectMapper objectMapper,
                       MeterRegistry meterRegistry) {
        this.config = properties.spill();
        this.producer = producer;
        this.objectMapper = objectMapper;
        this.spilledCounter = Counter.builder("mdg.spill.appended")
                .description("Events written to the disk buffer because the broker would not take them")
                .register(meterRegistry);
        this.drainedCounter = Counter.builder("mdg.spill.drained")
                .description("Buffered events later acknowledged by the broker")
                .register(meterRegistry);
        this.rejectedCounter = Counter.builder("mdg.spill.rejected")
                .description("Events the buffer could not take (full or unavailable); these go to the DLQ")
                .register(meterRegistry);
        this.unreadableCounter = Counter.builder("mdg.spill.unreadable")
                .description("Buffered records that could not be read back and were skipped")
                .register(meterRegistry);
        Gauge.builder("mdg.spill.pending", this, SpillBuffer::pending)
                .description("Events waiting in the disk buffer").register(meterRegistry);
        Gauge.builder("mdg.spill.bytes", this, SpillBuffer::bytes)
                .description("Disk used by the buffer").baseUnit("bytes").register(meterRegistry);
    }

    /**
     * True while events are waiting. Callers must then {@link #offer} new events rather than
     * publish them, or a newer event could overtake an older one of the same symbol.
     */
    public boolean isActive() {
        return pending() > 0;
    }

    /**
     * Takes ownership of an event the broker did not accept.
     *
     * @return {@code false} if the buffer is disabled, unavailable or full — the event is
     *         still the caller's problem
     */
    public boolean offer(CanonicalTradeEvent event) {
        SpillJournal current = journal;
        if (current == null) {
            rejectedCounter.increment();
            return false;
        }
        try {
            if (!current.append(objectMapper.writeValueAsBytes(event))) {
                rejectedCounter.increment();
                return false;
            }
        } catch (IOException | UncheckedIOException ex) {
            log.error("Could not buffer event {} to disk", event.eventId(), ex);
            rejectedCounter.increment();
            return false;
        }
        spilledCounter.increment();
        signal();
        return true;
    }

    public long pending() {
        SpillJournal current = journal;
        return current == null ? 0 : current.pending();
    }

    public long bytes() {
        SpillJournal current = journal;
        return current == null ? 0 : current.bytes();
    }

    // ------------------------------------------------------------------
    // Draining
    // ------------------------------------------------------------------

    private void drainLoop() {
        while (running) {
            try {
                if (pending() == 0) {
                    awaitWork();
                    continue;
                }
                if (!drainOnce()) {
                    TimeUnit.MILLISECONDS.sleep(config.retryInterval().toMillis());
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException ex) {
                // The drainer must outlive anything a single round can throw.
                log.error("Spill drain round failed; will retry", ex);
                sleepQuietly();
            }
        }
    }

    /** @return {@code false} if the broker is still refusing and the caller should back off */
    boolean drainOnce() {
        SpillJournal current = journal;
        List<byte[]> records = current.peek(config.drainBatchSize());
        if (records.isEmpty()) {
            return true;
        }

        // A leading run of unreadable records can only be skipped; anything after the first
        // readable one waits its turn.
        int skipped = 0;
        List<CanonicalTradeEvent> events = new ArrayList<>(records.size());
        for (byte[] record : records) {
            try {
                events.add(objectMapper.readValue(record, CanonicalTradeEvent.class));
            } catch (IOException | RuntimeException ex) {
                if (!events.isEmpty()) {
                    break;
                }
                skipped++;
                unreadableCounter.increment();
                log.error("Skipping an unreadable spill record ({} bytes)", record.length, ex);
            }
        }
        if (events.isEmpty()) {
            current.commit(skipped);
            return true;
        }

        // Probe with the first event through Retry + CircuitBreaker, so the breaker learns
        // the broker is back (or stays open and fails this fast while it is not).
        try {
            producer.publishOrThrow(events.get(0));
        } catch (RuntimeException ex) {
            log.debug("Spill drain: broker still unavailable ({})", ex.toString());
            current.commit(skipped);
            return false;
        }
        int delivered = 1;
        if (events.size() > 1) {
            delivered += producer.publishBatch(events.subList(1, events.size()));
        }
        current.commit(skipped + delivered);
        drainedCounter.increment(delivered);

        long left = current.pending();
        if (left == 0) {
            log.info("Spill buffer drained: broker caught up, direct publishing resumes");
        }
        return delivered == events.size();
    }

    private void awaitWork() throws InterruptedException {
        lock.lock();
        try {
            if (pending() == 0 && running) {
                work.await(1, TimeUnit.SECONDS);
            }
        } finally {
            lock.unlock();
        }
    }

    private void signal() {
        lock.lock();
        try {
            work.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void sleepQuietly() {
        try {
            TimeUnit.MILLISECONDS.sleep(config.retryInterval().toMillis());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    @Override
    public void start() {
        if (running) {
            return;
        }
        if (!config.enabled()) {
            log.info("Spill buffer disabled: events the broker refuses go to the DLQ");
            running = true;
            return;
        }
        try {
            journal = new SpillJournal(config.directory(), config.maxBytes(), config.segmentBytes(), config.fsync());
        } catch (IOException | RuntimeException ex) {
            // An unwritable directory must not stop the gateway from serving; it degrades to
            // the pre-spill behaviour (DLQ) and says so loudly.
            log.error("Spill buffer unavailable at {} — events the broker refuses will go to the DLQ",
                    config.directory().toAbsolutePath(), ex);
            running = true;
            return;
        }
        long waiting = journal.pending();
        if (waiting > 0) {
            log.warn("Spill buffer holds {} event(s) from a previous run; draining before direct publishing",
                    waiting);
        }
        running = true;
        drainer = Thread.ofVirtual().name("mdg-spill-drain").start(this::drainLoop);
    }

    @Override
    public void stop() {
        running = false;
        Thread thread = drainer;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(5_000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        SpillJournal current = journal;
        if (current != null) {
            long waiting = current.pending();
            if (waiting > 0) {
                log.warn("Shutting down with {} event(s) still in the spill buffer; they are on disk and "
                        + "will be published on the next start", waiting);
            }
            try {
                current.close();
            } catch (IOException ex) {
                log.warn("Could not close the spill journal cleanly", ex);
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * Starts before, and stops after, the lanes (MAX_VALUE - 2000): events still in flight at
     * shutdown must be able to land here when the broker is the thing that is down.
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 3000;
    }
}
