package com.mdg.gateway.resilience;

import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.producer.FailureStage;
import com.mdg.gateway.support.Fixtures;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Circuit breaker state machine and fallback routing, through the real Spring proxy.
 *
 * <p>The test profile pins {@code retry.max-attempts = 1} so one {@code publish()} is
 * exactly one breaker call — the state assertions below are then arithmetic rather than
 * guesswork. Retry itself is covered by {@link KafkaPublishRetryTest}.
 */
class CircuitBreakerFallbackTest extends AbstractResilienceTest {

    @Test
    @DisplayName("stays CLOSED and never dead-letters while the broker is healthy")
    void closedBreakerPublishesNormally() {
        reset(canonicalKafkaTemplate);
        when(canonicalKafkaTemplate.send(anyString(), anyString(), any(CanonicalTradeEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult()));

        for (int i = 0; i < 10; i++) {
            producer.publish(Fixtures.canonicalEvent());
        }

        assertThat(breaker().getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        verify(deadLetterPublisher, never()).publish(any(), any(), any(), any());
    }

    @Test
    @DisplayName("trips to OPEN once the failure rate is breached, then short-circuits")
    void breakerOpensAndThenRejectsWithoutTouchingTheBroker() {
        reset(canonicalKafkaTemplate);
        when(canonicalKafkaTemplate.send(anyString(), anyString(), any(CanonicalTradeEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new org.apache.kafka.common.errors.TimeoutException("broker unreachable")));

        // sliding-window = minimum-calls = 4, failure-rate-threshold = 50% (test profile).
        for (int i = 0; i < 4; i++) {
            assertThatCode(() -> producer.publish(Fixtures.canonicalEvent())).doesNotThrowAnyException();
        }

        assertThat(breaker().getState()).isEqualTo(CircuitBreaker.State.OPEN);
        verify(canonicalKafkaTemplate, times(4)).send(anyString(), anyString(), any());

        // Every failure was absorbed by the fallback and routed to the DLQ, not rethrown.
        verify(deadLetterPublisher, times(4)).publish(
                eq(Exchange.BINANCE), eq(Fixtures.BINANCE_TRADE), any(), eq(FailureStage.PUBLISH));

        // Now the valuable part: with the breaker open the broker is not called at all.
        clearInvocations(canonicalKafkaTemplate, deadLetterPublisher);

        assertThatCode(() -> producer.publish(Fixtures.canonicalEvent())).doesNotThrowAnyException();

        verify(canonicalKafkaTemplate, never()).send(anyString(), anyString(), any());
        ArgumentCaptor<Throwable> cause = ArgumentCaptor.forClass(Throwable.class);
        verify(deadLetterPublisher).publish(any(), any(), cause.capture(), eq(FailureStage.PUBLISH));

        // Proof the short-circuit — not another broker timeout — drove this dead letter.
        assertThat(cause.getValue()).isInstanceOf(CallNotPermittedException.class);
    }

    @Test
    @DisplayName("an open breaker is not retried — retry ignores CallNotPermittedException")
    void openBreakerIsNotRetried() {
        breaker().transitionToOpenState();
        clearInvocations(canonicalKafkaTemplate, deadLetterPublisher);

        producer.publish(Fixtures.canonicalEvent());

        // Without `ignore-exceptions: CallNotPermittedException` in the retry config, each
        // call would burn its full retry budget hammering a breaker that said "stop".
        verify(canonicalKafkaTemplate, never()).send(anyString(), anyString(), any());
        verify(deadLetterPublisher, times(1)).publish(any(), any(), any(), eq(FailureStage.PUBLISH));
    }

    @Test
    @DisplayName("HALF_OPEN closes again after the broker recovers")
    void breakerRecoversThroughHalfOpen() {
        breaker().transitionToOpenState();
        breaker().transitionToHalfOpenState();

        reset(canonicalKafkaTemplate);
        when(canonicalKafkaTemplate.send(anyString(), anyString(), any(CanonicalTradeEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult()));

        // permitted-number-of-calls-in-half-open-state = 2 (test profile)
        producer.publish(Fixtures.canonicalEvent());
        producer.publish(Fixtures.canonicalEvent());

        assertThat(breaker().getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        verify(canonicalKafkaTemplate, atLeastOnce()).send(anyString(), anyString(), any());
    }

    @Test
    @DisplayName("a half-open probe that fails re-opens the breaker")
    void breakerReopensWhenTheProbeFails() {
        breaker().transitionToOpenState();
        breaker().transitionToHalfOpenState();

        reset(canonicalKafkaTemplate);
        when(canonicalKafkaTemplate.send(anyString(), anyString(), any(CanonicalTradeEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new org.apache.kafka.common.errors.TimeoutException("still down")));

        producer.publish(Fixtures.canonicalEvent());
        producer.publish(Fixtures.canonicalEvent());

        assertThat(breaker().getState()).isEqualTo(CircuitBreaker.State.OPEN);
    }

    @Test
    @DisplayName("publishOrThrow propagates instead of dead-lettering, so replay stays safe")
    void publishOrThrowHasNoFallback() {
        reset(canonicalKafkaTemplate);
        when(canonicalKafkaTemplate.send(anyString(), anyString(), any(CanonicalTradeEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new org.apache.kafka.common.errors.TimeoutException("broker unreachable")));
        clearInvocations(deadLetterPublisher);

        assertThat(breaker().getState()).isEqualTo(CircuitBreaker.State.CLOSED);

        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> producer.publishOrThrow(Fixtures.canonicalEvent()))
                .isInstanceOf(RuntimeException.class);

        // If this dead-lettered, every DLQ replay against a broken broker would duplicate
        // its own input back into the DLQ.
        verify(deadLetterPublisher, never()).publish(any(), any(), any(), any());
    }

    private static SendResult<String, CanonicalTradeEvent> sendResult() {
        return new SendResult<>(
                new ProducerRecord<>("normalized-market-data", "BTC-USD", Fixtures.canonicalEvent()),
                new RecordMetadata(new TopicPartition("normalized-market-data", 0), 0L, 0, 0L, 0, 0));
    }
}
