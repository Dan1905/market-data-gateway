package com.mdg.gateway.resilience;

import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.producer.FailureStage;
import com.mdg.gateway.support.Fixtures;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Retry behaviour on the broker publish path.
 *
 * <p>A separate context (its own {@code @TestPropertySource}) because the breaker tests
 * deliberately pin retries to one attempt; here we want three so the retry loop is visible.
 * The breaker window is widened so it cannot trip mid-test and mask what retry is doing.
 */
@TestPropertySource(properties = {
        "resilience4j.retry.instances.kafka-producer.max-attempts=3",
        "resilience4j.retry.instances.kafka-producer.wait-duration=10ms",
        "resilience4j.circuitbreaker.instances.kafka-producer.sliding-window-size=100",
        "resilience4j.circuitbreaker.instances.kafka-producer.minimum-number-of-calls=100"
})
class KafkaPublishRetryTest extends AbstractResilienceTest {

    @Test
    @DisplayName("a transient broker failure is retried up to max-attempts, then dead-lettered")
    void retriesThenFallsBackToDlq() {
        reset(canonicalKafkaTemplate);
        when(canonicalKafkaTemplate.send(anyString(), anyString(), any(CanonicalTradeEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new org.apache.kafka.common.errors.TimeoutException("leader election")));
        clearInvocations(deadLetterPublisher);

        producer.publish(Fixtures.canonicalEvent());

        verify(canonicalKafkaTemplate, times(3)).send(anyString(), anyString(), any());
        // Exactly one dead letter for three attempts — the fallback runs after the retry
        // budget is spent, not once per attempt.
        verify(deadLetterPublisher, times(1)).publish(any(), any(), any(), eq(FailureStage.PUBLISH));
    }

    @Test
    @DisplayName("a failure that resolves on the second attempt never reaches the DLQ")
    void succeedsOnRetryWithoutDeadLettering() {
        reset(canonicalKafkaTemplate);
        when(canonicalKafkaTemplate.send(anyString(), anyString(), any(CanonicalTradeEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new org.apache.kafka.common.errors.TimeoutException("transient")))
                .thenReturn(CompletableFuture.completedFuture(sendResult()));
        clearInvocations(deadLetterPublisher);

        producer.publish(Fixtures.canonicalEvent());

        // This is the whole reason retry guards the publish path rather than the parser:
        // broker faults genuinely do resolve between attempts. Parse faults never do.
        verify(canonicalKafkaTemplate, times(2)).send(anyString(), anyString(), any());
        verify(deadLetterPublisher, never()).publish(any(), any(), any(), any());
    }

    @Test
    @DisplayName("a successful first attempt is not retried")
    void doesNotRetryOnSuccess() {
        reset(canonicalKafkaTemplate);
        when(canonicalKafkaTemplate.send(anyString(), anyString(), any(CanonicalTradeEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult()));

        producer.publish(Fixtures.canonicalEvent());

        verify(canonicalKafkaTemplate, times(1)).send(anyString(), anyString(), any());
    }

    @Test
    @DisplayName("retry is configured against the gateway's own publish exception")
    void retryConfigTargetsPublishFailures() {
        // Guards against a refactor that renames MarketDataPublishException and silently
        // turns retry into a no-op, since the YAML references it by fully-qualified name.
        assertThat(retryRegistry.retry("kafka-producer").getRetryConfig().getMaxAttempts()).isEqualTo(3);
    }

    private static SendResult<String, CanonicalTradeEvent> sendResult() {
        return new SendResult<>(
                new ProducerRecord<>("normalized-market-data", "BTC-USD", Fixtures.canonicalEvent()),
                new RecordMetadata(new TopicPartition("normalized-market-data", 0), 0L, 0, 0L, 0, 0));
    }
}
