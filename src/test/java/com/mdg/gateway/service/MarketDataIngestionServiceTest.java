package com.mdg.gateway.service;

import com.mdg.gateway.exception.PayloadParsingException;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.producer.DeadLetterPublisher;
import com.mdg.gateway.producer.FailureStage;
import com.mdg.gateway.producer.MarketDataProducer;
import com.mdg.gateway.producer.SpillBuffer;
import com.mdg.gateway.backfill.GapBackfillService;
import com.mdg.gateway.config.GatewayProperties;
import com.mdg.gateway.config.JacksonConfig;
import com.mdg.gateway.support.Fixtures;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.extension.ExtendWith;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * The deduplicator is REAL here, not mocked: claim/release ordering around a publish is the
 * behaviour under test, and a mock would only assert that methods were called.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MarketDataIngestionServiceTest {

    @Mock
    private PayloadTransformationService transformationService;

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

    /** Real, like the deduplicator. Disabled for most tests; see {@link #sequenced()}. */
    private TradeSequencer sequencer;
    private TradeDeduplicator deduplicator;
    private SymbolLaneDispatcher lanes;

    private MeterRegistry meterRegistry;
    private MarketDataIngestionService service;

    @TempDir
    Path dir;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        deduplicator = new TradeDeduplicator(Fixtures.gatewayProperties(), meterRegistry);
        // Same-thread executor: lanes run inline, so every assertion below sees a finished
        // delivery. Lane concurrency itself is covered by SymbolLaneDispatcherTest.
        lanes = new SymbolLaneDispatcher(
                Runnable::run, 100, Duration.ofSeconds(1), meterRegistry);
        // Fixtures' properties have backfill disabled: every event is UNTRACKED and the
        // sequencing step is a no-op, as for a venue whose ids cannot be sequenced.
        sequencer = new TradeSequencer(Fixtures.gatewayProperties(), JacksonConfig.marketDataObjectMapper());
        service = service(sequencer);
        when(producer.publish(any())).thenReturn(true);
    }

    private MarketDataIngestionService service(TradeSequencer tradeSequencer) {
        return new MarketDataIngestionService(transformationService, producer, deadLetterPublisher,
                deduplicator, lanes, spill, tradeSequencer, backfill, meterRegistry);
    }

    /** A service whose sequencer is switched on, keeping its positions under {@code dir}. */
    private MarketDataIngestionService sequenced() {
        GatewayProperties base = Fixtures.gatewayProperties();
        GatewayProperties.Backfill on = new GatewayProperties.Backfill(true, dir.resolve("positions.json"), 20_000,
                Duration.ofSeconds(2), 2, base.backfill().binanceUrl(), base.backfill().coinbaseUrl(),
                base.backfill().krakenUrl());
        sequencer = new TradeSequencer(new GatewayProperties(base.topics(), base.symbol(), base.producer(),
                base.dlq(), base.dedup(), base.lanes(), base.spill(), on), JacksonConfig.marketDataObjectMapper());
        return service(sequencer);
    }

    private static CanonicalTradeEvent trade(String venueSymbol, String normalized, String tradeId) {
        return Fixtures.canonicalEvent().toBuilder()
                .venueSymbol(venueSymbol)
                .symbol(normalized)
                .tradeId(tradeId)
                .eventId(CanonicalTradeEvent.deterministicEventId("BINANCE", venueSymbol, tradeId))
                .build();
    }

    private double deduplicated() {
        return meterRegistry.counter("mdg.events.deduplicated", "exchange", "BINANCE").count();
    }

    @Test
    void publishesEveryEventFromAFrame() {
        CanonicalTradeEvent first = trade("BTCUSDT", "BTC-USD", "1");
        CanonicalTradeEvent second = trade("ETHUSDT", "ETH-USD", "1");
        when(transformationService.transform(Exchange.COINBASE, Fixtures.COINBASE_TRADE_BATCH))
                .thenReturn(List.of(first, second));

        service.ingest(Exchange.COINBASE, Fixtures.COINBASE_TRADE_BATCH);

        verify(producer).publish(first);
        verify(producer).publish(second);
        assertThat(meterRegistry.counter("mdg.frames.received").count()).isEqualTo(1.0);
        verifyNoInteractions(deadLetterPublisher);
    }

    @Test
    @DisplayName("a parse failure goes straight to the DLQ, with no publish attempted")
    void deadLettersParseFailures() {
        PayloadParsingException failure = new PayloadParsingException(
                Exchange.BINANCE, Fixtures.MALFORMED_JSON, "Malformed BINANCE JSON");
        when(transformationService.transform(Exchange.BINANCE, Fixtures.MALFORMED_JSON))
                .thenThrow(failure);

        service.ingest(Exchange.BINANCE, Fixtures.MALFORMED_JSON);

        verify(deadLetterPublisher).publish(
                Exchange.BINANCE, Fixtures.MALFORMED_JSON, failure, FailureStage.TRANSFORM);
        verify(producer, never()).publish(any());
        assertThat(meterRegistry.counter("mdg.frames.transform.failed").count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("control frames are counted as skipped, not as failures")
    void countsSkippedControlFrames() {
        when(transformationService.transform(Exchange.KRAKEN, Fixtures.KRAKEN_HEARTBEAT))
                .thenReturn(List.of());

        service.ingest(Exchange.KRAKEN, Fixtures.KRAKEN_HEARTBEAT);

        assertThat(meterRegistry.counter("mdg.frames.skipped").count()).isEqualTo(1.0);
        assertThat(meterRegistry.counter("mdg.frames.transform.failed").count()).isZero();
        verify(producer, never()).publish(any());
        verifyNoInteractions(deadLetterPublisher);
    }

    @Test
    @DisplayName("an unexpected bug in transform is contained, not propagated")
    void deadLettersUnexpectedTransformErrors() {
        when(transformationService.transform(any(), any()))
                .thenThrow(new IllegalStateException("mapper bug"));

        // The caller is a WebSocket read loop; an escaping exception would kill the feed.
        assertThatCode(() -> service.ingest(Exchange.BINANCE, "{}")).doesNotThrowAnyException();

        verify(deadLetterPublisher).publish(eq(Exchange.BINANCE), eq("{}"), any(), eq(FailureStage.TRANSFORM));
    }

    @Test
    @DisplayName("one event failing to publish does not abort the rest of the batch")
    void continuesBatchWhenOnePublishEscapesItsFallback() {
        CanonicalTradeEvent first = trade("BTCUSDT", "BTC-USD", "10");
        CanonicalTradeEvent second = trade("ETHUSDT", "ETH-USD", "10");
        when(transformationService.transform(any(), any())).thenReturn(List.of(first, second));
        doThrow(new IllegalStateException("breaker misconfigured")).when(producer).publish(first);

        assertThatCode(() -> service.ingest(Exchange.COINBASE, "{}")).doesNotThrowAnyException();

        verify(producer).publish(second);
        verify(deadLetterPublisher).publish(eq(Exchange.COINBASE), eq("{}"), any(), eq(FailureStage.PUBLISH));
    }

    @Test
    @DisplayName("a frame still refused after the limiter wait is counted, not queued in heap")
    void throttleFallbackDropsTheFrame() {
        service.ingestThrottled(Exchange.BINANCE, Fixtures.BINANCE_TRADE,
                new RuntimeException("RateLimiter 'ingestion' does not permit further calls"));

        assertThat(meterRegistry.counter("mdg.frames.throttled").count()).isEqualTo(1.0);
        verifyNoInteractions(transformationService, deadLetterPublisher);
        verify(producer, never()).publish(any());
    }

    // ------------------------------------------------------------------
    // Broker outage: spill to disk
    // ------------------------------------------------------------------

    @Test
    @DisplayName("an event the broker refuses is spilled to disk, not dead-lettered")
    void undeliveredEventIsSpilled() {
        CanonicalTradeEvent trade = trade("BTCUSDT", "BTC-USD", "800");
        when(transformationService.transform(any(), any())).thenReturn(List.of(trade));
        when(producer.publish(trade)).thenReturn(false);
        when(spill.offer(trade)).thenReturn(true);

        service.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);
        // The venue re-sends it (a reconnect snapshot, say) while it still waits on disk.
        service.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);

        verify(spill, times(1)).offer(trade);
        verifyNoInteractions(deadLetterPublisher);
        // The claim is KEPT: the event is on its way, so the second copy is a duplicate.
        assertThat(deduplicated()).isEqualTo(1.0);
        assertThat(meterRegistry.counter("mdg.events.undeliverable").count()).isZero();
    }

    @Test
    @DisplayName("while older events wait on disk, new ones queue behind them instead of publishing")
    void activeSpillTakesEverythingToPreserveOrder() {
        CanonicalTradeEvent trade = trade("BTCUSDT", "BTC-USD", "801");
        when(transformationService.transform(any(), any())).thenReturn(List.of(trade));
        when(spill.isActive()).thenReturn(true);
        when(spill.offer(trade)).thenReturn(true);

        service.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);

        // Publishing it directly, even to a healthy broker, would put it on the topic ahead
        // of older trades of the same symbol that are still in the buffer.
        verify(producer, never()).publish(any());
        verify(spill).offer(trade);
        verifyNoInteractions(deadLetterPublisher);
    }

    @Test
    @DisplayName("a full spill buffer falls back to the DLQ and frees the event for a later copy")
    void fullSpillDeadLetters() {
        CanonicalTradeEvent trade = trade("BTCUSDT", "BTC-USD", "802");
        when(transformationService.transform(any(), any())).thenReturn(List.of(trade));
        when(spill.isActive()).thenReturn(true);
        when(spill.offer(trade)).thenReturn(false);

        service.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);
        service.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);

        verify(deadLetterPublisher, times(2)).publish(
                eq(Exchange.BINANCE), eq(Fixtures.BINANCE_TRADE), any(), eq(FailureStage.PUBLISH));
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
        MarketDataIngestionService sequenced = sequenced();
        CanonicalTradeEvent t100 = trade("BTCUSDT", "BTC-USD", "100");
        CanonicalTradeEvent t101 = trade("BTCUSDT", "BTC-USD", "101").toBuilder().backfilled(true).build();
        CanonicalTradeEvent t102 = trade("BTCUSDT", "BTC-USD", "102").toBuilder().backfilled(true).build();
        CanonicalTradeEvent t103 = trade("BTCUSDT", "BTC-USD", "103");
        when(transformationService.transform(any(), eq("frame-100"))).thenReturn(List.of(t100));
        when(transformationService.transform(any(), eq("frame-103"))).thenReturn(List.of(t103));
        when(backfill.recover(eq(t103), any())).thenReturn(List.of(t101, t102));

        sequenced.ingest(Exchange.BINANCE, "frame-100");
        sequenced.ingest(Exchange.BINANCE, "frame-103");   // 101 and 102 happened while we were away

        InOrder order = inOrder(producer);
        order.verify(producer).publish(t100);
        order.verify(producer).publish(t101);
        order.verify(producer).publish(t102);
        order.verify(producer).publish(t103);

        ArgumentCaptor<TradeSequencer.Check> gap = ArgumentCaptor.forClass(TradeSequencer.Check.class);
        verify(backfill).recover(eq(t103), gap.capture());
        assertThat(gap.getValue().firstMissing()).isEqualTo(101);
        assertThat(gap.getValue().lastMissing()).isEqualTo(102);
        assertThat(sequencer.positions().get("BINANCE:BTCUSDT").tradeId()).isEqualTo(103);
    }

    @Test
    @DisplayName("consecutive trades never call the venue's REST API")
    void noGapNoBackfill() {
        MarketDataIngestionService sequenced = sequenced();
        when(transformationService.transform(any(), any())).thenReturn(
                List.of(trade("BTCUSDT", "BTC-USD", "100"), trade("BTCUSDT", "BTC-USD", "101"),
                        trade("BTCUSDT", "BTC-USD", "102")));

        sequenced.ingest(Exchange.BINANCE, "{}");

        verify(producer, times(3)).publish(any());
        verifyNoInteractions(backfill);
    }

    @Test
    @DisplayName("if the venue cannot supply the missing trades, the live trade is still delivered")
    void failedBackfillDoesNotBlockTheFeed() {
        MarketDataIngestionService sequenced = sequenced();
        CanonicalTradeEvent t100 = trade("BTCUSDT", "BTC-USD", "100");
        CanonicalTradeEvent t200 = trade("BTCUSDT", "BTC-USD", "200");
        when(transformationService.transform(any(), eq("a"))).thenReturn(List.of(t100));
        when(transformationService.transform(any(), eq("b"))).thenReturn(List.of(t200));
        when(backfill.recover(any(), any())).thenReturn(List.of());

        sequenced.ingest(Exchange.BINANCE, "a");
        sequenced.ingest(Exchange.BINANCE, "b");

        verify(producer).publish(t200);
        // The position moves on: the next trade must not re-trigger the same recovery.
        assertThat(sequencer.check(trade("BTCUSDT", "BTC-USD", "201")).verdict())
                .isEqualTo(TradeSequencer.Verdict.NEXT);
    }

    @Test
    @DisplayName("trades re-sent after a restart are dropped using the saved position, with an empty dedup window")
    void replayedTradesAfterRestartAreDropped() {
        MarketDataIngestionService before = sequenced();
        when(transformationService.transform(any(), eq("live"))).thenReturn(
                List.of(trade("BTC-USD", "BTC-USD", "500"), trade("BTC-USD", "BTC-USD", "501")));
        before.ingest(Exchange.COINBASE, "live");
        sequencer.start();
        sequencer.stop();   // shutdown: positions are saved

        // A new process: new deduplicator (nothing remembered), positions loaded from disk.
        deduplicator = new TradeDeduplicator(Fixtures.gatewayProperties(), meterRegistry);
        MarketDataIngestionService after = sequenced();
        // Coinbase's snapshot on reconnect: two trades we already published, then a new one.
        when(transformationService.transform(any(), eq("snapshot"))).thenReturn(List.of(
                trade("BTC-USD", "BTC-USD", "500"), trade("BTC-USD", "BTC-USD", "501"),
                trade("BTC-USD", "BTC-USD", "502")));

        after.ingest(Exchange.COINBASE, "snapshot");

        verify(producer, times(1)).publish(trade("BTC-USD", "BTC-USD", "500"));
        verify(producer, times(1)).publish(trade("BTC-USD", "BTC-USD", "501"));
        verify(producer, times(1)).publish(trade("BTC-USD", "BTC-USD", "502"));
        assertThat(meterRegistry.counter("mdg.events.already.delivered").count()).isEqualTo(2.0);
        verifyNoInteractions(backfill);
    }

    // ------------------------------------------------------------------
    // Deduplication
    // ------------------------------------------------------------------

    @Test
    @DisplayName("the same trade arriving three times is published exactly once")
    void duplicateTradesArePublishedOnce() {
        CanonicalTradeEvent trade = trade("BTCUSDT", "BTC-USD", "500");
        when(transformationService.transform(any(), any())).thenReturn(List.of(trade));

        service.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);
        service.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);
        service.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);

        assertThat(meterRegistry.counter("mdg.frames.received").count()).isEqualTo(3.0);
        verify(producer, times(1)).publish(any());
        assertThat(deduplicated()).isEqualTo(2.0);
    }

    @Test
    @DisplayName("a trade whose publish FAILED is not remembered, so its next copy still gets through")
    void failedDeliveryReleasesTheClaim() {
        CanonicalTradeEvent trade = trade("BTCUSDT", "BTC-USD", "600");
        when(transformationService.transform(any(), any())).thenReturn(List.of(trade));
        // First attempt: retries exhausted / breaker open -> fallback -> not delivered.
        when(producer.publish(trade)).thenReturn(false).thenReturn(true);

        service.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);
        service.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);

        // If the failed attempt had kept its claim, the second copy - now the trade's only route
        // to the topic - would have been discarded as a duplicate of something never delivered.
        verify(producer, times(2)).publish(trade);
        assertThat(deduplicated()).isZero();
    }

    @Test
    @DisplayName("a publish that throws also releases its claim")
    void escapedPublishReleasesTheClaim() {
        CanonicalTradeEvent trade = trade("BTCUSDT", "BTC-USD", "700");
        when(transformationService.transform(any(), any())).thenReturn(List.of(trade));
        when(producer.publish(trade)).thenThrow(new IllegalStateException("boom")).thenReturn(true);

        service.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);
        service.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);

        verify(producer, times(2)).publish(trade);
    }

    @Test
    @DisplayName("the same trade id on two different venue symbols is NOT a duplicate")
    void sameTradeIdDifferentVenueSymbolIsNotADuplicate() {
        CanonicalTradeEvent usdt = trade("BTCUSDT", "BTC-USD", "42");
        CanonicalTradeEvent usdc = trade("BTCUSDC", "BTC-USD", "42");
        when(transformationService.transform(any(), any())).thenReturn(List.of(usdt, usdc));

        service.ingest(Exchange.BINANCE, "{}");

        verify(producer).publish(usdt);
        verify(producer).publish(usdc);
        assertThat(deduplicated()).isZero();
    }
}
