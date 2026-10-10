package com.mdg.gateway.service;

import com.mdg.gateway.exception.PayloadParsingException;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
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

    private MeterRegistry meterRegistry;
    private MarketDataIngestionService service;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        TradeDeduplicator deduplicator = new TradeDeduplicator(Fixtures.gatewayProperties(), meterRegistry);
        // Same-thread executor: lanes run inline, so every assertion below sees a finished
        // delivery. Lane concurrency itself is covered by SymbolLaneDispatcherTest.
        SymbolLaneDispatcher lanes = new SymbolLaneDispatcher(
                Runnable::run, 100, Duration.ofSeconds(1), meterRegistry);
        service = new MarketDataIngestionService(
                transformationService, producer, deadLetterPublisher, deduplicator, lanes, spill, meterRegistry);
        when(producer.publish(any())).thenReturn(true);
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
