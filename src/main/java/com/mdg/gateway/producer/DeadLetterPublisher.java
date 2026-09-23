package com.mdg.gateway.producer;

import com.mdg.gateway.config.GatewayProperties;
import com.mdg.gateway.model.Exchange;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;

/**
 * Terminal sink for anything the pipeline could not process.
 *
 * <p><b>This class never throws.</b> It is the last line of defence, and a DLQ publish
 * that propagates would take down the ingest path that called it — turning one bad frame
 * into a dead venue connection. A failure here is logged at ERROR, counted, and swallowed.
 *
 * <p>The raw frame is written verbatim as the record value. Replay depends on that: the
 * body must be re-parseable by the same code that originally rejected it, once the bug or
 * schema drift that caused the rejection is fixed.
 */
@Component
public class DeadLetterPublisher {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterPublisher.class);

    private final KafkaTemplate<String, String> dlqKafkaTemplate;
    private final GatewayProperties properties;
    private final Counter published;
    private final Counter failed;

    public DeadLetterPublisher(KafkaTemplate<String, String> dlqKafkaTemplate,
                               GatewayProperties properties,
                               MeterRegistry meterRegistry) {
        this.dlqKafkaTemplate = dlqKafkaTemplate;
        this.properties = properties;
        this.published = Counter.builder("mdg.dlq.published")
                .description("Records written to the dead letter queue")
                .register(meterRegistry);
        this.failed = Counter.builder("mdg.dlq.publish.failed")
                .description("Dead letter publishes that themselves failed — data loss")
                .register(meterRegistry);
    }

    public void publish(Exchange exchange, String rawPayload, Throwable cause, FailureStage stage) {
        publish(exchange, rawPayload, cause, stage, 0);
    }

    /**
     * @param replayAttempts how many replays this record has already survived; carried so a
     *                       poison record cannot cycle through replay forever unnoticed
     */
    public void publish(Exchange exchange,
                        String rawPayload,
                        Throwable cause,
                        FailureStage stage,
                        int replayAttempts) {
        String exchangeName = exchange == null ? "UNKNOWN" : exchange.name();
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    properties.topics().deadLetter(),
                    null,
                    exchangeName,
                    rawPayload == null ? "" : rawPayload);

            Headers headers = record.headers();
            addHeader(headers, DlqHeaders.EXCEPTION_MESSAGE, describe(cause));
            addHeader(headers, DlqHeaders.EXCEPTION_CLASS,
                    cause == null ? "unknown" : cause.getClass().getName());
            addHeader(headers, DlqHeaders.SOURCE_EXCHANGE, exchangeName);
            addHeader(headers, DlqHeaders.TIMESTAMP, Instant.now().toString());
            addHeader(headers, DlqHeaders.ORIGINAL_TOPIC, properties.topics().normalized());
            addHeader(headers, DlqHeaders.FAILURE_STAGE, stage.name());
            addHeader(headers, DlqHeaders.REPLAY_ATTEMPTS, Integer.toString(replayAttempts));

            // Fire-and-forget with an async error callback. Blocking here would add broker
            // latency to a path that is already handling a failure.
            dlqKafkaTemplate.send(record).whenComplete((result, error) -> {
                if (error != null) {
                    failed.increment();
                    log.error("DLQ publish failed for exchange={} — record is lost. payload={}",
                            exchangeName, truncate(rawPayload), error);
                } else {
                    published.increment();
                }
            });

            log.warn("Dead-lettered {} frame at stage={}: {}", exchangeName, stage, describe(cause));

        } catch (Exception ex) {
            // Serialization, buffer exhaustion, template shut down mid-request, etc.
            failed.increment();
            log.error("DLQ publish threw synchronously for exchange={} — record is lost. payload={}",
                    exchangeName, truncate(rawPayload), ex);
        }
    }

    private static void addHeader(Headers headers, String key, String value) {
        headers.add(key, Objects.requireNonNullElse(value, "").getBytes(StandardCharsets.UTF_8));
    }

    private static String describe(Throwable cause) {
        if (cause == null) {
            return "unspecified failure";
        }
        // Resilience4j and Kafka both wrap liberally; the root cause is what an operator needs.
        Throwable root = cause;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        String summary = message == null || message.isBlank() ? root.getClass().getSimpleName() : message;
        // Kafka rejects header values over the frame size; keep them comfortably small.
        return summary.length() > 512 ? summary.substring(0, 512) + "…" : summary;
    }

    private static String truncate(String payload) {
        if (payload == null) {
            return "<null>";
        }
        return payload.length() <= 256 ? payload : payload.substring(0, 256) + "…";
    }
}
