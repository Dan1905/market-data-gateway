package com.mdg.gateway.service;

import com.mdg.gateway.backfill.GapBackfillService;
import com.mdg.gateway.config.GatewayProperties;
import com.mdg.gateway.config.JacksonConfig;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.producer.DeadLetterPublisher;
import com.mdg.gateway.producer.FailureStage;
import com.mdg.gateway.producer.MarketDataProducer;
import com.mdg.gateway.producer.SpillBuffer;
import com.mdg.gateway.support.Fixtures;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * What happens to one trade between "normalized" and "on the topic".
 *
 * <p>The deduplicator and the sequencer are REAL here, not mocked: their interplay with
 * publishing (when a claim is kept or released, when a position moves) is the behaviour under
 * test, and mocks would only assert that methods were called. The edges are mocked: the
 * broker, the disk buffer, the venue's REST API and the dead letter queue.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TradeDeliveryPipelineTest {

    private static final String FRAME = Fixtures.BINANCE_TRADE;

    @Mock
    private MarketDataProducer producer;

    @Mock
    private DeadLetterPublisher deadLetterPublisher;

    /**
     * Unstubbed it refuses everything (offer -> false, isActive -> false): the "no spill
     * buffer available" case, where an undelivered event falls through to the DLQ.
     */
    @Mock
    private SpillBuffer spill;

    @Mock
    private GapBackfillService backfill;

    @TempDir
    Path dir;

    private MeterRegistry meterRegistry;
    private TradeDeduplicator deduplicator;
    private TradeSequencer sequencer;
    private TradeDeliveryPipeline pipeline;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        deduplicator = new TradeDeduplicator(Fixtures.gatewayProperties(), meterRegistry);
        // Fixtures' properties have backfill disabled: every event is UNTRACKED and the
        // sequencing step is a no-op, as for a venue whose ids cannot be sequenced.
        sequencer = new TradeSequencer(Fixtures.gatewayProperties(), JacksonConfig.marketDataObjectMapper());
        pipeline = pipeline();
        when(producer.publish(any())).thenReturn(true);
    }

    private TradeDeliveryPipeline pipeline() {
        return new TradeDeliveryPipeline(sequencer, backfill, deduplicator, producer, spill,
                deadLetterPublisher, meterRegistry);
    }

    /** Switches sequencing on, keeping positions under {@code dir}; simulates a process start. */
    private TradeDeliveryPipeline sequenced() {
        GatewayProperties base = Fixtures.gatewayProperties();
        GatewayProperties.Backfill on = new GatewayProperties.Backfill(true, dir.resolve("positions.json"), 20_000,
                Duration.ofSeconds(2), 2);
        sequencer = new TradeSequencer(new GatewayProperties(base.topics(), base.symbol(), base.producer(),
                base.dlq(), base.dedup(), base.lanes(), base.spill(), on), JacksonConfig.marketDataObjectMapper());
        return pipeline();
    }

    private static CanonicalTradeEvent trade(String venueSymbol, String tradeId) {
        return Fixtures.canonicalEvent().toBuilder()
                .venueSymbol(venueSymbol)
                .tradeId(tradeId)
                .eventId(CanonicalTradeEvent.deterministicEventId("BINANCE", venueSymbol, tradeId))
                .build();
    }

    private static CanonicalTradeEvent btc(long tradeId) {
        return trade("BTCUSDT", Long.toString(tradeId));
    }

    private double deduplicated() {
        return meterRegistry.counter("mdg.events.deduplicated", "exchange", "BINANCE").count();
    }

    // ------------------------------------------------------------------
    // The ordinary path
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a trade the broker accepts is published once and nothing else happens")
    void publishes() {
        pipeline.deliver(Exchange.BINANCE, FRAME, btc(1));

        verify(producer).publish(btc(1));
        verify(spill, never()).offer(any());
        verifyNoInteractions(deadLetterPublisher, backfill);
    }

    @Test
    @DisplayName("a publish that escapes its fallback is contained and dead-lettered, not thrown into the lane")
    void escapedPublishIsContained() {
        when(producer.publish(btc(10))).thenThrow(new IllegalStateException("breaker misconfigured"));

        assertThatCode(() -> pipeline.deliver(Exchange.BINANCE, FRAME, btc(10))).doesNotThrowAnyException();
        pipeline.deliver(Exchange.BINANCE, FRAME, btc(11));

        verify(producer).publish(btc(11));
        verify(deadLetterPublisher).publish(eq(Exchange.BINANCE), eq(FRAME), any(), eq(FailureStage.PUBLISH));
    }

    // ------------------------------------------------------------------
    // Broker outage: spill to disk
    // ------------------------------------------------------------------

    @Test
    @DisplayName("an event the broker refuses is spilled to disk, not dead-lettered")
    void undeliveredEventIsSpilled() {
        when(producer.publish(btc(800))).thenReturn(false);
        when(spill.offer(btc(800))).thenReturn(true);

        pipeline.deliver(Exchange.BINANCE, FRAME, btc(800));
        // The venue re-sends it (a reconnect snapshot, say) while it still waits on disk.
        pipeline.deliver(Exchange.BINANCE, FRAME, btc(800));

        verify(spill, times(1)).offer(btc(800));
        verifyNoInteractions(deadLetterPublisher);
        // The claim is KEPT: the event is on its way, so the second copy is a duplicate.
        assertThat(deduplicated()).isEqualTo(1.0);
        assertThat(meterRegistry.counter("mdg.events.undeliverable").count()).isZero();
    }

    @Test
    @DisplayName("while older events wait on disk, new ones queue behind them instead of publishing")
    void activeSpillTakesEverythingToPreserveOrder() {
        when(spill.isActive()).thenReturn(true);
        when(spill.offer(btc(801))).thenReturn(true);

        pipeline.deliver(Exchange.BINANCE, FRAME, btc(801));

        // Publishing it directly, even to a healthy broker, would put it on the topic ahead
        // of older trades of the same symbol that are still in the buffer.
        verify(producer, never()).publish(any());
        verify(spill).offer(btc(801));
        verifyNoInteractions(deadLetterPublisher);
    }

    @Test
    @DisplayName("a full spill buffer falls back to the DLQ and frees the event for a later copy")
    void fullSpillDeadLetters() {
        when(spill.isActive()).thenReturn(true);
        when(spill.offer(btc(802))).thenReturn(false);

        pipeline.deliver(Exchange.BINANCE, FRAME, btc(802));
        pipeline.deliver(Exchange.BINANCE, FRAME, btc(802));

        verify(deadLetterPublisher, times(2)).publish(
                eq(Exchange.BINANCE), eq(FRAME), any(), eq(FailureStage.PUBLISH));
        assertThat(meterRegistry.counter("mdg.events.undeliverable").count()).isEqualTo(2.0);
        // Released, so neither copy was swallowed as a duplicate of something never delivered.
        assertThat(deduplicated()).isZero();
    }

    // ------------------------------------------------------------------
    // Gaps: trades missed while disconnected
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a gap in trade ids is filled from the venue BEFORE the live trade is published")
    void gapIsBackfilledInOrder() {
        TradeDeliveryPipeline sequenced = sequenced();
        CanonicalTradeEvent t101 = btc(101).toBuilder().backfilled(true).build();
        CanonicalTradeEvent t102 = btc(102).toBuilder().backfilled(true).build();
        when(backfill.recover(eq(btc(103)), any())).thenReturn(List.of(t101, t102));

        sequenced.deliver(Exchange.BINANCE, FRAME, btc(100));
        sequenced.deliver(Exchange.BINANCE, FRAME, btc(103));   // 101 and 102 happened while we were away

        InOrder order = inOrder(producer);
        order.verify(producer).publish(btc(100));
        order.verify(producer).publish(t101);
        order.verify(producer).publish(t102);
        order.verify(producer).publish(btc(103));

        ArgumentCaptor<TradeSequencer.Check> gap = ArgumentCaptor.forClass(TradeSequencer.Check.class);
        verify(backfill).recover(eq(btc(103)), gap.capture());
        assertThat(gap.getValue().firstMissing()).isEqualTo(101);
        assertThat(gap.getValue().lastMissing()).isEqualTo(102);
        assertThat(sequencer.positions().get("BINANCE:BTCUSDT").tradeId()).isEqualTo(103);
    }

    @Test
    @DisplayName("consecutive trades never call the venue's REST API")
    void noGapNoBackfill() {
        TradeDeliveryPipeline sequenced = sequenced();

        sequenced.deliver(Exchange.BINANCE, FRAME, btc(100));
        sequenced.deliver(Exchange.BINANCE, FRAME, btc(101));
        sequenced.deliver(Exchange.BINANCE, FRAME, btc(102));

        verify(producer, times(3)).publish(any());
        verifyNoInteractions(backfill);
    }

    @Test
    @DisplayName("if the venue cannot supply the missing trades, the live trade is still delivered")
    void failedBackfillDoesNotBlockTheFeed() {
        TradeDeliveryPipeline sequenced = sequenced();
        when(backfill.recover(any(), any())).thenReturn(List.of());

        sequenced.deliver(Exchange.BINANCE, FRAME, btc(100));
        sequenced.deliver(Exchange.BINANCE, FRAME, btc(200));

        verify(producer).publish(btc(200));
        // The position moves on: the next trade must not re-trigger the same recovery.
        assertThat(sequencer.check(btc(201)).verdict()).isEqualTo(TradeSequencer.Verdict.NEXT);
    }

    @Test
    @DisplayName("trades re-sent after a restart are dropped using the saved position, with an empty dedup window")
    void replayedTradesAfterRestartAreDropped() {
        TradeDeliveryPipeline before = sequenced();
        before.deliver(Exchange.BINANCE, FRAME, btc(500));
        before.deliver(Exchange.BINANCE, FRAME, btc(501));
        sequencer.start();
        sequencer.stop();   // shutdown: positions are saved

        // A new process: new deduplicator (nothing remembered), positions loaded from disk.
        deduplicator = new TradeDeduplicator(Fixtures.gatewayProperties(), meterRegistry);
        TradeDeliveryPipeline after = sequenced();

        // The venue's snapshot on reconnect: two trades already published, then a new one.
        after.deliver(Exchange.BINANCE, FRAME, btc(500));
        after.deliver(Exchange.BINANCE, FRAME, btc(501));
        after.deliver(Exchange.BINANCE, FRAME, btc(502));

        verify(producer, times(1)).publish(btc(500));
        verify(producer, times(1)).publish(btc(501));
        verify(producer, times(1)).publish(btc(502));
        assertThat(meterRegistry.counter("mdg.events.already.delivered").count()).isEqualTo(2.0);
        verifyNoInteractions(backfill);
    }

    // ------------------------------------------------------------------
    // Deduplication
    // ------------------------------------------------------------------

    @Test
    @DisplayName("the same trade arriving three times is published exactly once")
    void duplicateTradesArePublishedOnce() {
        pipeline.deliver(Exchange.BINANCE, FRAME, btc(500));
        pipeline.deliver(Exchange.BINANCE, FRAME, btc(500));
        pipeline.deliver(Exchange.BINANCE, FRAME, btc(500));

        verify(producer, times(1)).publish(any());
        assertThat(deduplicated()).isEqualTo(2.0);
    }

    @Test
    @DisplayName("a trade that was never delivered is not remembered, so its next copy still gets through")
    void failedDeliveryReleasesTheClaim() {
        // First attempt: retries exhausted / breaker open, and no spill buffer to hold it.
        when(producer.publish(btc(600))).thenReturn(false).thenReturn(true);

        pipeline.deliver(Exchange.BINANCE, FRAME, btc(600));
        pipeline.deliver(Exchange.BINANCE, FRAME, btc(600));

        // If the failed attempt had kept its claim, the second copy - now the trade's only route
        // to the topic - would have been discarded as a duplicate of something never delivered.
        verify(producer, times(2)).publish(btc(600));
        assertThat(deduplicated()).isZero();
    }

    @Test
    @DisplayName("a publish that throws also releases its claim")
    void escapedPublishReleasesTheClaim() {
        when(producer.publish(btc(700))).thenThrow(new IllegalStateException("boom")).thenReturn(true);

        pipeline.deliver(Exchange.BINANCE, FRAME, btc(700));
        pipeline.deliver(Exchange.BINANCE, FRAME, btc(700));

        verify(producer, times(2)).publish(btc(700));
    }

    @Test
    @DisplayName("the same trade id on two different venue symbols is NOT a duplicate")
    void sameTradeIdDifferentVenueSymbolIsNotADuplicate() {
        pipeline.deliver(Exchange.BINANCE, FRAME, trade("BTCUSDT", "42"));
        pipeline.deliver(Exchange.BINANCE, FRAME, trade("BTCUSDC", "42"));

        verify(producer).publish(trade("BTCUSDT", "42"));
        verify(producer).publish(trade("BTCUSDC", "42"));
        assertThat(deduplicated()).isZero();
    }
}
