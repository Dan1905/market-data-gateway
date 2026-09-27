package com.mdg.gateway.service;

import com.mdg.gateway.exception.PayloadParsingException;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.producer.DeadLetterPublisher;
import com.mdg.gateway.producer.FailureStage;
import com.mdg.gateway.producer.MarketDataProducer;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Pipeline entry point: one raw venue frame in, zero or more published events out.
 *
 * <p>Called from a virtual thread owned by {@code ingestionExecutor}, never from the JDK
 * {@code HttpClient}'s receive thread — that separation is what keeps the socket draining
 * while a slow broker ack is in flight.
 *
 * <h2>Rate limiting and backpressure</h2>
 * The rate limiter caps how many frames per second this box will process, protecting a small
 * instance's CPU from a venue burst (a large liquidation prints hundreds of trades in a few
 * milliseconds). Over the limit the caller <em>waits</em> for a permit. The caller is the
 * socket's read loop, so while it waits no further frames are read: the burst stays in the
 * kernel's TCP buffer and then on the venue's side, instead of in heap or on the floor. The
 * per-symbol lanes apply the same brake when the broker, not the CPU, is the slow part.
 *
 * <p>Only a frame still refused after the full wait is dropped and counted in
 * {@code mdg.frames.throttled}. That means sustained overload, and it should page someone.
 */
@Service
public class MarketDataIngestionService {

    private static final Logger log = LoggerFactory.getLogger(MarketDataIngestionService.class);

    public static final String RESILIENCE_INSTANCE = "ingestion";

    private final PayloadTransformationService transformationService;
    private final MarketDataProducer producer;
    private final DeadLetterPublisher deadLetterPublisher;
    private final TradeDeduplicator deduplicator;
    private final SymbolLaneDispatcher lanes;
    private final Counter receivedCounter;
    private final Counter skippedCounter;
    private final Counter throttledCounter;
    private final Counter transformFailedCounter;

    public MarketDataIngestionService(PayloadTransformationService transformationService,
                                      MarketDataProducer producer,
                                      DeadLetterPublisher deadLetterPublisher,
                                      TradeDeduplicator deduplicator,
                                      SymbolLaneDispatcher lanes,
                                      MeterRegistry meterRegistry) {
        this.transformationService = transformationService;
        this.producer = producer;
        this.deadLetterPublisher = deadLetterPublisher;
        this.deduplicator = deduplicator;
        this.lanes = lanes;
        this.receivedCounter = Counter.builder("mdg.frames.received")
                .description("Raw frames handed to the pipeline").register(meterRegistry);
        this.skippedCounter = Counter.builder("mdg.frames.skipped")
                .description("Control frames (heartbeat, ack, status) — not failures")
                .register(meterRegistry);
        this.throttledCounter = Counter.builder("mdg.frames.throttled")
                .description("Frames dropped by the ingest rate limiter").register(meterRegistry);
        this.transformFailedCounter = Counter.builder("mdg.frames.transform.failed")
                .description("Frames that failed normalization and were dead-lettered")
                .register(meterRegistry);
    }

    /**
     * Normalizes and publishes a single frame.
     *
     * <p>Does not throw: every failure mode terminates either in the DLQ or in a counter.
     * The caller is a WebSocket read loop, and an exception escaping here would tear down a
     * venue connection over a single bad frame.
     */
    @RateLimiter(name = RESILIENCE_INSTANCE, fallbackMethod = "ingestThrottled")
    public void ingest(Exchange exchange, String rawFrame) {
        receivedCounter.increment();

        List<CanonicalTradeEvent> events;
        try {
            events = transformationService.transform(exchange, rawFrame);
        } catch (PayloadParsingException ex) {
            transformFailedCounter.increment();
            deadLetterPublisher.publish(exchange, rawFrame, ex, FailureStage.TRANSFORM);
            return;
        } catch (RuntimeException ex) {
            // Defensive: an unanticipated bug in transform must still not kill the socket.
            transformFailedCounter.increment();
            log.error("Unexpected failure transforming {} frame", exchange, ex);
            deadLetterPublisher.publish(exchange, rawFrame, ex, FailureStage.TRANSFORM);
            return;
        }

        if (events.isEmpty()) {
            skippedCounter.increment();
            return;
        }

        // Parsing happened here, in frame order. Delivery is handed to the event's lane: strictly
        // ordered within its venue symbol, concurrent across symbols. submit() blocks when the
        // global in-flight budget is spent, which is what keeps TCP backpressure intact.
        for (CanonicalTradeEvent event : events) {
            lanes.submit(laneKey(event), () -> deliver(exchange, rawFrame, event));
        }
    }

    /** Ordering is only meaningful within one venue symbol's trade-id sequence. */
    static String laneKey(CanonicalTradeEvent event) {
        return event.exchange() + ':' + event.venueSymbol();
    }

    private void deliver(Exchange exchange, String rawFrame, CanonicalTradeEvent event) {
        // Claim before publishing so two copies of one trade cannot both get through.
        if (!deduplicator.claim(event)) {
            return;
        }
        // publish() is proxied: Retry + CircuitBreaker apply, and its own fallback handles
        // failure. Nothing should surface here, but a breaker misconfiguration would, and it
        // must not break the lane.
        boolean delivered;
        try {
            delivered = producer.publish(event);
        } catch (RuntimeException ex) {
            log.error("Publish escaped its fallback for event {}", event.eventId(), ex);
            deadLetterPublisher.publish(exchange, rawFrame, ex, FailureStage.PUBLISH);
            delivered = false;
        }
        if (!delivered) {
            // Never delivered, so it must not count as "seen": a later copy (a replay, a
            // backfill, a reconnect snapshot) is then the event's only route to the topic.
            deduplicator.release(event);
        }
    }

    /**
     * Rate limiter fallback. Signature mirrors {@link #ingest} plus the trailing throwable.
     */
    @SuppressWarnings("unused")
    void ingestThrottled(Exchange exchange, String rawFrame, Throwable throwable) {
        throttledCounter.increment();
        if (log.isDebugEnabled()) {
            log.debug("Ingest throttled for {}: {}", exchange, throwable.getMessage());
        }
    }
}
