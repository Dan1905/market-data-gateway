package com.mdg.gateway.service;

import com.mdg.gateway.exception.PayloadParsingException;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.producer.DeadLetterPublisher;
import com.mdg.gateway.producer.FailureStage;
import com.mdg.gateway.support.Fixtures;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * The front of the pipeline: admit a frame, normalize it, hand each trade to its lane.
 *
 * <p>What happens to a trade after the hand-off is {@link TradeDeliveryPipelineTest}'s subject,
 * so delivery is a mock here.
 */
@ExtendWith(MockitoExtension.class)
class MarketDataIngestionServiceTest {

    @Mock
    private PayloadTransformationService transformationService;

    @Mock
    private TradeDeliveryPipeline delivery;

    @Mock
    private DeadLetterPublisher deadLetterPublisher;

    private MeterRegistry meterRegistry;
    private MarketDataIngestionService service;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        // Same-thread executor: lanes run inline, so every assertion below sees a finished
        // hand-off. Lane concurrency itself is covered by SymbolLaneDispatcherTest.
        SymbolLaneDispatcher lanes = new SymbolLaneDispatcher(
                Runnable::run, 100, Duration.ofSeconds(1), meterRegistry);
        service = new MarketDataIngestionService(
                transformationService, lanes, delivery, deadLetterPublisher, meterRegistry);
    }

    private static CanonicalTradeEvent trade(String exchange, String venueSymbol, String tradeId) {
        return Fixtures.canonicalEvent().toBuilder()
                .exchange(exchange)
                .venueSymbol(venueSymbol)
                .tradeId(tradeId)
                .eventId(CanonicalTradeEvent.deterministicEventId(exchange, venueSymbol, tradeId))
                .build();
    }

    @Test
    @DisplayName("every trade in a frame is handed to delivery, in frame order, with its source frame")
    void handsEveryEventToDelivery() {
        CanonicalTradeEvent first = trade("COINBASE", "BTC-USD", "1");
        CanonicalTradeEvent second = trade("COINBASE", "BTC-USD", "2");
        CanonicalTradeEvent other = trade("COINBASE", "ETH-USD", "1");
        when(transformationService.transform(Exchange.COINBASE, Fixtures.COINBASE_TRADE_BATCH))
                .thenReturn(List.of(first, second, other));

        service.ingest(Exchange.COINBASE, Fixtures.COINBASE_TRADE_BATCH);

        InOrder order = inOrder(delivery);
        order.verify(delivery).deliver(Exchange.COINBASE, Fixtures.COINBASE_TRADE_BATCH, first);
        order.verify(delivery).deliver(Exchange.COINBASE, Fixtures.COINBASE_TRADE_BATCH, second);
        order.verify(delivery).deliver(Exchange.COINBASE, Fixtures.COINBASE_TRADE_BATCH, other);
        assertThat(meterRegistry.counter("mdg.frames.received").count()).isEqualTo(1.0);
        verifyNoInteractions(deadLetterPublisher);
    }

    @Test
    @DisplayName("the lane is the venue symbol: same symbol on two venues, or two symbols on one, do not share")
    void laneKeyIsExchangePlusVenueSymbol() {
        assertThat(MarketDataIngestionService.laneKey(trade("BINANCE", "BTCUSDT", "1"))).isEqualTo("BINANCE:BTCUSDT");
        assertThat(MarketDataIngestionService.laneKey(trade("BINANCE", "BTCUSDC", "1"))).isEqualTo("BINANCE:BTCUSDC");
        assertThat(MarketDataIngestionService.laneKey(trade("KRAKEN", "BTC/USD", "1"))).isEqualTo("KRAKEN:BTC/USD");
    }

    @Test
    @DisplayName("a parse failure goes straight to the DLQ, with nothing delivered")
    void deadLettersParseFailures() {
        PayloadParsingException failure = new PayloadParsingException(
                Exchange.BINANCE, Fixtures.MALFORMED_JSON, "Malformed BINANCE JSON");
        when(transformationService.transform(Exchange.BINANCE, Fixtures.MALFORMED_JSON))
                .thenThrow(failure);

        service.ingest(Exchange.BINANCE, Fixtures.MALFORMED_JSON);

        verify(deadLetterPublisher).publish(
                Exchange.BINANCE, Fixtures.MALFORMED_JSON, failure, FailureStage.TRANSFORM);
        verifyNoInteractions(delivery);
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
        verifyNoInteractions(delivery, deadLetterPublisher);
    }

    @Test
    @DisplayName("an unexpected bug in transform is contained, not propagated")
    void deadLettersUnexpectedTransformErrors() {
        when(transformationService.transform(any(), any()))
                .thenThrow(new IllegalStateException("mapper bug"));

        // The caller is a WebSocket read loop; an escaping exception would kill the feed.
        assertThatCode(() -> service.ingest(Exchange.BINANCE, "{}")).doesNotThrowAnyException();

        verify(deadLetterPublisher).publish(eq(Exchange.BINANCE), eq("{}"), any(), eq(FailureStage.TRANSFORM));
        verifyNoInteractions(delivery);
    }

    @Test
    @DisplayName("a frame still refused after the limiter wait is counted, not queued in heap")
    void throttleFallbackDropsTheFrame() {
        service.ingestThrottled(Exchange.BINANCE, Fixtures.BINANCE_TRADE,
                new RuntimeException("RateLimiter 'ingestion' does not permit further calls"));

        assertThat(meterRegistry.counter("mdg.frames.throttled").count()).isEqualTo(1.0);
        verifyNoInteractions(transformationService, delivery, deadLetterPublisher);
    }
}
