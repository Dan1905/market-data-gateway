package com.mdg.gateway.service;

import com.mdg.gateway.backfill.GapBackfillService;
import com.mdg.gateway.exception.MarketDataPublishException;
import com.mdg.gateway.exception.PayloadParsingException;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.producer.DeadLetterPublisher;
import com.mdg.gateway.producer.FailureStage;
import com.mdg.gateway.producer.MarketDataProducer;
import com.mdg.gateway.producer.SpillBuffer;
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
    private final SpillBuffer spill;
    private final TradeSequencer sequencer;
    private final GapBackfillService backfill;
    private final Counter receivedCounter;
    private final Counter skippedCounter;
    private final Counter throttledCounter;
    private final Counter transformFailedCounter;
    private final Counter undeliverableCounter;
    private final Counter alreadyDeliveredCounter;

    public MarketDataIngestionService(PayloadTransformationService transformationService,
                                      MarketDataProducer producer,
                                      DeadLetterPublisher deadLetterPublisher,
                                      TradeDeduplicator deduplicator,
                                      SymbolLaneDispatcher lanes,
                                      SpillBuffer spill,
                                      TradeSequencer sequencer,
                                      GapBackfillService backfill,
                                      MeterRegistry meterRegistry) {
        this.transformationService = transformationService;
        this.producer = producer;
        this.deadLetterPublisher = deadLetterPublisher;
        this.deduplicator = deduplicator;
        this.lanes = lanes;
        this.spill = spill;
        this.sequencer = sequencer;
        this.backfill = backfill;
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
        this.undeliverableCounter = Counter.builder("mdg.events.undeliverable")
                .description("Events neither the broker nor the spill buffer could take; dead-lettered")
                .register(meterRegistry);
        this.alreadyDeliveredCounter = Counter.builder("mdg.events.already.delivered")
                .description("Events at or below the last delivered trade id for their symbol; dropped")
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

    /**
     * Runs in the event's lane, so for one venue symbol this is strictly sequential — which
     * is what lets a gap be filled <em>before</em> the trade that revealed it is published.
     */
    private void deliver(Exchange exchange, String rawFrame, CanonicalTradeEvent event) {
        TradeSequencer.Check sequence = sequencer.check(event);
        switch (sequence.verdict()) {
            case BEHIND -> {
                // At or below the last trade delivered for this symbol: a venue re-sending
                // recent trades after a reconnect. Known from the saved position, so this
                // still works when a restart has emptied the deduplication window.
                alreadyDeliveredCounter.increment();
                return;
            }
            case GAP -> {
                // Trades happened that we never saw. Fetch them and publish them first; the
                // lane keeps this symbol's later trades waiting behind this one meanwhile.
                for (CanonicalTradeEvent missed : backfill.recover(event, sequence)) {
                    publish(exchange, missed.rawPayload(), missed);
                    sequencer.advance(missed);
                }
            }
            default -> {
                // NEXT, or a symbol whose ids are not numeric and so cannot be sequenced.
            }
        }
        publish(exchange, rawFrame, event);
        sequencer.advance(event);
    }

    private void publish(Exchange exchange, String rawFrame, CanonicalTradeEvent event) {
        // Claim before publishing so two copies of one trade cannot both get through.
        if (!deduplicator.claim(event)) {
            return;
        }
        // Earlier events are waiting on disk for the broker. This one must queue behind
        // them, or it would reach the topic before an older trade of the same symbol.
        if (spill.isActive()) {
            if (!spill.offer(event)) {
                giveUp(exchange, rawFrame, event);
            }
            return;
        }
        // publish() is proxied: Retry + CircuitBreaker apply, and it returns false rather
        // than throwing. Nothing should surface here, but a breaker misconfiguration would,
        // and it must not break the lane.
        boolean delivered;
        try {
            delivered = producer.publish(event);
        } catch (RuntimeException ex) {
            log.error("Publish escaped its fallback for event {}", event.eventId(), ex);
            delivered = false;
        }
        // The broker would not take it: keep it on disk until it will. The dedup claim
        // stays, because the event is still on its way to the topic.
        if (!delivered && !spill.offer(event)) {
            giveUp(exchange, rawFrame, event);
        }
    }

    /**
     * Last resort when the broker refused an event and the spill buffer could not hold it
     * either (full, disabled, or its disk is unwritable): dead-letter the original frame.
     */
    private void giveUp(Exchange exchange, String rawFrame, CanonicalTradeEvent event) {
        undeliverableCounter.increment();
        deadLetterPublisher.publish(exchange, rawFrame,
                new MarketDataPublishException(
                        "Broker unavailable and spill buffer could not take event " + event.eventId(), null),
                FailureStage.PUBLISH);
        // Never delivered, so it must not count as "seen": a later copy (a replay, a
        // backfill, a reconnect snapshot) is then the event's only route to the topic.
        deduplicator.release(event);
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
