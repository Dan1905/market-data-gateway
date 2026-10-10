package com.mdg.gateway.service;

import com.mdg.gateway.backfill.GapBackfillService;
import com.mdg.gateway.exception.MarketDataPublishException;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.producer.DeadLetterPublisher;
import com.mdg.gateway.producer.FailureStage;
import com.mdg.gateway.producer.MarketDataProducer;
import com.mdg.gateway.producer.SpillBuffer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Everything that happens to one normalized trade on its way to the topic.
 *
 * <p>The steps, in the order they run:
 * <ol>
 *   <li><b>Sequence.</b> Is this the next trade for its symbol, an old one being re-sent, or
 *       has something been missed? ({@link TradeSequencer})</li>
 *   <li><b>Recover.</b> If trades were missed, fetch them from the venue and deliver them
 *       first. ({@link GapBackfillService})</li>
 *   <li><b>Deduplicate.</b> Claim the trade's deterministic id so a second copy cannot also
 *       get through. ({@link TradeDeduplicator})</li>
 *   <li><b>Publish</b> to the broker, or, if the broker will not take it, <b>spill</b> it to
 *       disk to be published in order later. ({@link MarketDataProducer}, {@link SpillBuffer})</li>
 *   <li><b>Give up</b> to the dead letter queue only if neither could take it.</li>
 * </ol>
 *
 * <p>{@link #deliver} must be called from the event's lane (see {@link SymbolLaneDispatcher}):
 * for one venue symbol it then runs strictly one event at a time, which is what makes
 * "recover the gap, then publish the trade that revealed it" keep the topic in order, and
 * what makes the sequencer's check-then-advance safe without a lock.
 *
 * <p>It never throws. A trade that cannot be delivered ends in the spill buffer, the DLQ or
 * a counter, never in an exception that would stall its lane.
 */
@Component
public class TradeDeliveryPipeline {

    private static final Logger log = LoggerFactory.getLogger(TradeDeliveryPipeline.class);

    private final TradeSequencer sequencer;
    private final GapBackfillService backfill;
    private final TradeDeduplicator deduplicator;
    private final MarketDataProducer producer;
    private final SpillBuffer spill;
    private final DeadLetterPublisher deadLetterPublisher;
    private final Counter undeliverableCounter;
    private final Counter alreadyDeliveredCounter;
    private final Counter sequenceRestartCounter;

    public TradeDeliveryPipeline(TradeSequencer sequencer,
                                 GapBackfillService backfill,
                                 TradeDeduplicator deduplicator,
                                 MarketDataProducer producer,
                                 SpillBuffer spill,
                                 DeadLetterPublisher deadLetterPublisher,
                                 MeterRegistry meterRegistry) {
        this.sequencer = sequencer;
        this.backfill = backfill;
        this.deduplicator = deduplicator;
        this.producer = producer;
        this.spill = spill;
        this.deadLetterPublisher = deadLetterPublisher;
        this.undeliverableCounter = Counter.builder("mdg.events.undeliverable")
                .description("Events neither the broker nor the spill buffer could take; dead-lettered")
                .register(meterRegistry);
        this.alreadyDeliveredCounter = Counter.builder("mdg.events.already.delivered")
                .description("Events at or below the last delivered trade id for their symbol; dropped")
                .register(meterRegistry);
        this.sequenceRestartCounter = Counter.builder("mdg.sequence.restarts")
                .description("Times a venue's trade ids for a symbol started again from a much lower number")
                .register(meterRegistry);
    }

    /**
     * Delivers one event, first recovering any trades missed before it.
     *
     * @param rawFrame the venue frame the event was parsed from, kept for the dead letter queue
     */
    public void deliver(Exchange exchange, String rawFrame, CanonicalTradeEvent event) {
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
            case RESTARTED -> sequenceRestartCounter.increment();
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
}
