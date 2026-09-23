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
 * <h2>Rate limiting</h2>
 * The limiter is an admission control valve, not a queue. Venues can burst far above their
 * steady-state rate (a large liquidation prints hundreds of trades in a few milliseconds),
 * and on a 1 GB box the failure mode of absorbing that burst is a heap death spiral. When
 * the limiter refuses, the frame is <em>dropped and counted</em> rather than queued: for
 * market data, the freshest tick is the valuable one and a backlog of stale ticks is worse
 * than a gap. Alert on {@code mdg.frames.throttled} and raise the limit if it fires.
 */
@Service
public class MarketDataIngestionService {

    private static final Logger log = LoggerFactory.getLogger(MarketDataIngestionService.class);

    public static final String RESILIENCE_INSTANCE = "ingestion";

    private final PayloadTransformationService transformationService;
    private final MarketDataProducer producer;
    private final DeadLetterPublisher deadLetterPublisher;
    private final Counter receivedCounter;
    private final Counter skippedCounter;
    private final Counter throttledCounter;
    private final Counter transformFailedCounter;

    public MarketDataIngestionService(PayloadTransformationService transformationService,
                                      MarketDataProducer producer,
                                      DeadLetterPublisher deadLetterPublisher,
                                      MeterRegistry meterRegistry) {
        this.transformationService = transformationService;
        this.producer = producer;
        this.deadLetterPublisher = deadLetterPublisher;
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

        for (CanonicalTradeEvent event : events) {
            // publish() is proxied: Retry + CircuitBreaker apply, and its own fallback
            // routes to the DLQ. Nothing should surface here, but a breaker misconfiguration
            // would, and it must not abort the remaining events in this batch.
            try {
                producer.publish(event);
            } catch (RuntimeException ex) {
                log.error("Publish escaped its fallback for event {}", event.eventId(), ex);
                deadLetterPublisher.publish(exchange, rawFrame, ex, FailureStage.PUBLISH);
            }
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
