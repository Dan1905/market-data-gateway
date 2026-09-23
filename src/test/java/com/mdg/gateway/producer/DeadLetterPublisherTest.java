package com.mdg.gateway.producer;

import com.mdg.gateway.exception.PayloadParsingException;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.support.Fixtures;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DeadLetterPublisherTest {

    @Mock
    private KafkaTemplate<String, String> dlqKafkaTemplate;

    @Captor
    private ArgumentCaptor<ProducerRecord<String, String>> recordCaptor;

    private MeterRegistry meterRegistry;
    private DeadLetterPublisher publisher;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        publisher = new DeadLetterPublisher(dlqKafkaTemplate, Fixtures.gatewayProperties(), meterRegistry);
    }

    @Test
    @DisplayName("writes the raw frame verbatim so a replay re-runs the real path")
    void publishesRawPayloadUnmodified() {
        when(dlqKafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(Exchange.BINANCE, Fixtures.MALFORMED_JSON,
                new PayloadParsingException(Exchange.BINANCE, Fixtures.MALFORMED_JSON, "boom"),
                FailureStage.TRANSFORM);

        verify(dlqKafkaTemplate).send(recordCaptor.capture());
        ProducerRecord<String, String> record = recordCaptor.getValue();

        assertThat(record.topic()).isEqualTo("market-data-dlq");
        assertThat(record.key()).isEqualTo("BINANCE");
        assertThat(record.value()).isEqualTo(Fixtures.MALFORMED_JSON);
    }

    @Test
    @DisplayName("carries the full error context as headers")
    void populatesErrorContextHeaders() {
        when(dlqKafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(Exchange.KRAKEN, "{\"bad\":true}",
                new IllegalArgumentException("price is required"), FailureStage.TRANSFORM);

        verify(dlqKafkaTemplate).send(recordCaptor.capture());
        ProducerRecord<String, String> record = recordCaptor.getValue();

        assertThat(header(record, DlqHeaders.EXCEPTION_MESSAGE)).isEqualTo("price is required");
        assertThat(header(record, DlqHeaders.EXCEPTION_CLASS))
                .isEqualTo(IllegalArgumentException.class.getName());
        assertThat(header(record, DlqHeaders.SOURCE_EXCHANGE)).isEqualTo("KRAKEN");
        assertThat(header(record, DlqHeaders.ORIGINAL_TOPIC)).isEqualTo("normalized-market-data");
        assertThat(header(record, DlqHeaders.FAILURE_STAGE)).isEqualTo("TRANSFORM");
        assertThat(header(record, DlqHeaders.REPLAY_ATTEMPTS)).isEqualTo("0");
        assertThat(Instant.parse(header(record, DlqHeaders.TIMESTAMP))).isNotNull();
    }

    @Test
    @DisplayName("unwraps to the root cause so operators see the real reason")
    void headerCarriesRootCauseMessage() {
        when(dlqKafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        Throwable wrapped = new RuntimeException("outer",
                new IllegalStateException("middle", new IllegalArgumentException("the actual reason")));

        publisher.publish(Exchange.BINANCE, "{}", wrapped, FailureStage.PUBLISH);

        verify(dlqKafkaTemplate).send(recordCaptor.capture());
        assertThat(header(recordCaptor.getValue(), DlqHeaders.EXCEPTION_MESSAGE))
                .isEqualTo("the actual reason");
    }

    @Test
    @DisplayName("truncates oversized messages to stay under Kafka's header limits")
    void truncatesLongExceptionMessages() {
        when(dlqKafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(Exchange.BINANCE, "{}", new RuntimeException("x".repeat(5_000)),
                FailureStage.TRANSFORM);

        verify(dlqKafkaTemplate).send(recordCaptor.capture());
        assertThat(header(recordCaptor.getValue(), DlqHeaders.EXCEPTION_MESSAGE)).hasSizeLessThan(600);
    }

    @Test
    @DisplayName("never throws when the broker rejects the DLQ write")
    void swallowsSynchronousSendFailure() {
        // This is the last line of defence. If it propagated, one bad frame would tear down
        // the venue connection that produced it.
        when(dlqKafkaTemplate.send(any(ProducerRecord.class)))
                .thenThrow(new IllegalStateException("producer closed"));

        assertThatCode(() -> publisher.publish(Exchange.BINANCE, "{}",
                new RuntimeException("original"), FailureStage.TRANSFORM))
                .doesNotThrowAnyException();

        assertThat(meterRegistry.counter("mdg.dlq.publish.failed").count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("counts an asynchronously failed DLQ write as data loss")
    void countsAsynchronousSendFailure() {
        when(dlqKafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        assertThatCode(() -> publisher.publish(Exchange.BINANCE, "{}",
                new RuntimeException("original"), FailureStage.TRANSFORM))
                .doesNotThrowAnyException();

        assertThat(meterRegistry.counter("mdg.dlq.publish.failed").count()).isEqualTo(1.0);
        assertThat(meterRegistry.counter("mdg.dlq.published").count()).isZero();
    }

    @Test
    @DisplayName("handles a null exchange and a null payload without blowing up")
    void toleratesMissingContext() {
        when(dlqKafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(null, null, null, FailureStage.TRANSFORM);

        verify(dlqKafkaTemplate).send(recordCaptor.capture());
        ProducerRecord<String, String> record = recordCaptor.getValue();

        assertThat(record.key()).isEqualTo("UNKNOWN");
        assertThat(record.value()).isEmpty();
        assertThat(header(record, DlqHeaders.EXCEPTION_MESSAGE)).isEqualTo("unspecified failure");
    }

    @Test
    void recordsReplayAttemptCount() {
        when(dlqKafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(Exchange.COINBASE, "{}", new RuntimeException("again"),
                FailureStage.TRANSFORM, 3);

        verify(dlqKafkaTemplate).send(recordCaptor.capture());
        assertThat(header(recordCaptor.getValue(), DlqHeaders.REPLAY_ATTEMPTS)).isEqualTo("3");
    }

    private static String header(ProducerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        assertThat(header).as("header %s", name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
