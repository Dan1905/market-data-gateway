package com.mdg.gateway.producer;

import com.mdg.gateway.exception.MarketDataPublishException;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.support.Fixtures;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Plain-Mockito tests of the publish mechanics. Resilience4j's annotations are inert here
 * (no proxy), which is exactly what we want: these assert what the method itself does.
 * {@code CircuitBreakerFallbackTest} covers the annotated behaviour with a real proxy.
 */
@ExtendWith(MockitoExtension.class)
class MarketDataProducerTest {

    @Mock
    private KafkaTemplate<String, CanonicalTradeEvent> kafkaTemplate;

    @Mock
    private DeadLetterPublisher deadLetterPublisher;

    private MeterRegistry meterRegistry;
    private MarketDataProducer producer;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        producer = new MarketDataProducer(
                kafkaTemplate, deadLetterPublisher, Fixtures.gatewayProperties(), meterRegistry);
    }

    @Test
    @DisplayName("keys by symbol so all venues for one instrument share a partition")
    void publishesKeyedBySymbol() {
        when(kafkaTemplate.send(any(String.class), any(String.class), any(CanonicalTradeEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult()));

        CanonicalTradeEvent event = Fixtures.canonicalEvent();
        producer.publish(event);

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("normalized-market-data"), key.capture(), eq(event));

        assertThat(key.getValue()).isEqualTo("BTC-USD");
        assertThat(meterRegistry.counter("mdg.events.published").count()).isEqualTo(1.0);
        verifyNoInteractions(deadLetterPublisher);
    }

    @Test
    @DisplayName("records publish latency for the slow-call breaker to act on")
    void recordsLatency() {
        when(kafkaTemplate.send(any(String.class), any(String.class), any(CanonicalTradeEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult()));

        producer.publish(Fixtures.canonicalEvent());

        assertThat(meterRegistry.timer("mdg.events.publish.latency").count()).isEqualTo(1L);
    }

    @Test
    @DisplayName("wraps a broker rejection so the breaker can classify it")
    void translatesBrokerFailure() {
        when(kafkaTemplate.send(any(String.class), any(String.class), any(CanonicalTradeEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new org.apache.kafka.common.errors.TimeoutException("no leader")));

        assertThatThrownBy(() -> producer.publish(Fixtures.canonicalEvent()))
                .isInstanceOf(MarketDataPublishException.class)
                .hasMessageContaining("Broker rejected event")
                .hasRootCauseMessage("no leader");
    }

    @Test
    @DisplayName("times out rather than parking a virtual thread forever")
    void translatesAckTimeout() {
        // A future that never completes: without the bounded get() this would hang the
        // ingest task and, with it, the socket's demand for the next frame.
        when(kafkaTemplate.send(any(String.class), any(String.class), any(CanonicalTradeEvent.class)))
                .thenReturn(new CompletableFuture<>());

        MarketDataProducer impatient = new MarketDataProducer(
                kafkaTemplate, deadLetterPublisher,
                new com.mdg.gateway.config.GatewayProperties(
                        Fixtures.gatewayProperties().topics(),
                        Fixtures.gatewayProperties().symbol(),
                        new com.mdg.gateway.config.GatewayProperties.Producer(java.time.Duration.ofMillis(50)),
                        Fixtures.gatewayProperties().dlq()),
                meterRegistry);

        assertThatThrownBy(() -> impatient.publish(Fixtures.canonicalEvent()))
                .isInstanceOf(MarketDataPublishException.class)
                .hasMessageContaining("timed out");
    }

    @Test
    @DisplayName("fallback routes the original venue frame to the DLQ")
    void fallbackDeadLettersOriginalFrame() {
        CanonicalTradeEvent event = Fixtures.canonicalEvent();
        RuntimeException cause = new MarketDataPublishException("broker gone", new RuntimeException());

        producer.publishFallback(event, cause);

        verify(deadLetterPublisher).publish(
                eq(Exchange.BINANCE),
                eq(Fixtures.BINANCE_TRADE),   // the raw frame, not a re-serialized canonical event
                eq(cause),
                eq(FailureStage.PUBLISH));
        assertThat(meterRegistry.counter("mdg.events.publish.fallback").count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("fallback still dead-letters when the raw frame was not retained")
    void fallbackHandlesMissingRawPayload() {
        CanonicalTradeEvent event = Fixtures.canonicalEvent().toBuilder().rawPayload(null).build();

        producer.publishFallback(event, new RuntimeException("broker gone"));

        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(deadLetterPublisher).publish(
                eq(Exchange.BINANCE), payload.capture(), any(), eq(FailureStage.PUBLISH));

        assertThat(payload.getValue()).contains("BTC-USD");
    }

    @Test
    @DisplayName("publishOrThrow has no fallback so replay can count real failures")
    void publishOrThrowPropagatesInsteadOfDeadLettering() {
        when(kafkaTemplate.send(any(String.class), any(String.class), any(CanonicalTradeEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("still down")));

        assertThatThrownBy(() -> producer.publishOrThrow(Fixtures.canonicalEvent()))
                .isInstanceOf(MarketDataPublishException.class);

        // The crucial property: a replay against a broken broker must not grow the DLQ.
        verify(deadLetterPublisher, never()).publish(any(), any(), any(), any());
    }

    private static SendResult<String, CanonicalTradeEvent> sendResult() {
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("normalized-market-data", 0), 0L, 0, 0L, 0, 0);
        return new SendResult<>(
                new ProducerRecord<>("normalized-market-data", "BTC-USD", Fixtures.canonicalEvent()),
                metadata);
    }
}
