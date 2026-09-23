package com.mdg.gateway.service;

import com.mdg.gateway.exception.PayloadParsingException;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.producer.DeadLetterPublisher;
import com.mdg.gateway.producer.FailureStage;
import com.mdg.gateway.producer.MarketDataProducer;
import com.mdg.gateway.support.Fixtures;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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

@ExtendWith(MockitoExtension.class)
class MarketDataIngestionServiceTest {

    @Mock
    private PayloadTransformationService transformationService;

    @Mock
    private MarketDataProducer producer;

    @Mock
    private DeadLetterPublisher deadLetterPublisher;

    private MeterRegistry meterRegistry;
    private MarketDataIngestionService service;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        service = new MarketDataIngestionService(
                transformationService, producer, deadLetterPublisher, meterRegistry);
    }

    @Test
    void publishesEveryEventFromAFrame() {
        CanonicalTradeEvent first = Fixtures.canonicalEvent();
        CanonicalTradeEvent second = Fixtures.canonicalEvent().toBuilder().symbol("ETH-USD").build();
        when(transformationService.transform(Exchange.COINBASE, Fixtures.COINBASE_TICKER_BATCH))
                .thenReturn(List.of(first, second));

        service.ingest(Exchange.COINBASE, Fixtures.COINBASE_TICKER_BATCH);

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
        verifyNoInteractions(producer);
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
        verifyNoInteractions(producer, deadLetterPublisher);
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
        CanonicalTradeEvent first = Fixtures.canonicalEvent();
        CanonicalTradeEvent second = Fixtures.canonicalEvent().toBuilder().symbol("ETH-USD").build();
        when(transformationService.transform(any(), any())).thenReturn(List.of(first, second));
        doThrow(new IllegalStateException("breaker misconfigured")).when(producer).publish(first);

        assertThatCode(() -> service.ingest(Exchange.COINBASE, "{}")).doesNotThrowAnyException();

        verify(producer).publish(second);
        verify(deadLetterPublisher).publish(eq(Exchange.COINBASE), eq("{}"), any(), eq(FailureStage.PUBLISH));
    }

    @Test
    @DisplayName("throttled frames are dropped and counted, never queued")
    void throttleFallbackDropsTheFrame() {
        service.ingestThrottled(Exchange.BINANCE, Fixtures.BINANCE_TRADE,
                new RuntimeException("RateLimiter 'ingestion' does not permit further calls"));

        // Stale ticks are worth less than fresh ones, so a burst is shed rather than buffered.
        assertThat(meterRegistry.counter("mdg.frames.throttled").count()).isEqualTo(1.0);
        verifyNoInteractions(transformationService, producer, deadLetterPublisher);
    }

    @Test
    void countsEveryFrameExactlyOnce() {
        when(transformationService.transform(any(), any())).thenReturn(List.of(Fixtures.canonicalEvent()));

        service.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);
        service.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);
        service.ingest(Exchange.BINANCE, Fixtures.BINANCE_TRADE);

        assertThat(meterRegistry.counter("mdg.frames.received").count()).isEqualTo(3.0);
        verify(producer, times(3)).publish(any());
        verify(deadLetterPublisher, never()).publish(any(), any(), any(), any());
    }
}
