package com.mdg.gateway.producer;

import com.mdg.gateway.config.GatewayProperties;
import com.mdg.gateway.exception.MarketDataPublishException;
import com.mdg.gateway.model.CanonicalTradeEvent;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes canonical events to the primary topic under Retry + CircuitBreaker.
 *
 * <h2>Why the resilience lives here and not on the parser</h2>
 * Retrying is only rational against a <em>transient</em> fault. A malformed frame is
 * deterministic — parsing it three times yields three identical failures while delaying
 * the DLQ write and holding the ingest path open. Broker unavailability is the opposite:
 * leader elections, rolling restarts and network blips resolve in seconds. So Retry and
 * the CircuitBreaker guard the broker call, and parse failures short-circuit straight to
 * the DLQ.
 *
 * <h2>Why this method blocks</h2>
 * {@code KafkaTemplate.send} returns immediately; its future completes on the producer's
 * I/O thread. If we returned that future, Resilience4j's synchronous aspects would record
 * every send as an instant success and the circuit breaker would never open. Blocking on
 * the ack is what makes failure observable — and on a virtual thread that park releases
 * the carrier, so the cost is a continuation, not an OS thread.
 *
 * <h2>Aspect ordering</h2>
 * Resilience4j orders CircuitBreaker inside Retry, so each retry attempt is itself gated
 * by the breaker. Once the breaker is open it raises {@code CallNotPermittedException},
 * which the retry config lists under {@code ignore-exceptions} — otherwise we would burn
 * the full retry budget hammering a breaker that is telling us to stop. The fallback then
 * reports the failure, and the ingestion path spills the event to disk (see
 * {@code SpillBuffer}) to be published in order once the broker is back.
 */
@Component
public class MarketDataProducer {

    private static final Logger log = LoggerFactory.getLogger(MarketDataProducer.class);

    public static final String RESILIENCE_INSTANCE = "kafka-producer";

    private final KafkaTemplate<String, CanonicalTradeEvent> kafkaTemplate;
    private final GatewayProperties properties;
    private final Counter publishedCounter;
    private final Counter fallbackCounter;
    private final Timer publishTimer;

    public MarketDataProducer(KafkaTemplate<String, CanonicalTradeEvent> kafkaTemplate,
                              GatewayProperties properties,
                              MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.publishedCounter = Counter.builder("mdg.events.published")
                .description("Canonical events acknowledged by the broker")
                .register(meterRegistry);
        this.fallbackCounter = Counter.builder("mdg.events.publish.fallback")
                .description("Publishes that exhausted retries or hit an open breaker")
                .register(meterRegistry);
        this.publishTimer = Timer.builder("mdg.events.publish.latency")
                .description("Time from send to broker acknowledgement")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
    }

    /**
     * Publishes one event, blocking until the broker acknowledges it.
     *
     * @return {@code true} if the broker acknowledged the event; {@code false} if retries were
     *         exhausted or the breaker was open, in which case the caller still owns the event. Callers need the
     *         distinction: a deduplication claim on an event that was never delivered must be
     *         released, or a later copy would be dropped as a duplicate of nothing.
     * @throws MarketDataPublishException only if invoked outside the proxy (the fallback
     *                                    normally absorbs it)
     */
    @Retry(name = RESILIENCE_INSTANCE, fallbackMethod = "publishFallback")
    @CircuitBreaker(name = RESILIENCE_INSTANCE)
    public boolean publish(CanonicalTradeEvent event) {
        doPublish(event);
        return true;
    }

    /**
     * Same guarantees as {@link #publish} but with no fallback — failures propagate.
     *
     * <p>This exists for DLQ replay. If replay used {@link #publish}, a still-broken broker
     * would send each replayed record <em>back</em> to the DLQ, so every run would duplicate
     * its own input and the queue would grow monotonically. Replay instead lets the failure
     * surface, counts it, and leaves the original record untouched at its offset.
     */
    @Retry(name = RESILIENCE_INSTANCE)
    @CircuitBreaker(name = RESILIENCE_INSTANCE)
    public void publishOrThrow(CanonicalTradeEvent event) {
        doPublish(event);
    }

    private void doPublish(CanonicalTradeEvent event) {
        long startNanos = System.nanoTime();
        try {
            CompletableFuture<SendResult<String, CanonicalTradeEvent>> future =
                    kafkaTemplate.send(properties.topics().normalized(), event.partitionKey(), event);

            SendResult<String, CanonicalTradeEvent> result =
                    future.get(properties.producer().sendTimeout().toMillis(), TimeUnit.MILLISECONDS);

            publishedCounter.increment();
            publishTimer.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);

            if (log.isTraceEnabled()) {
                log.trace("Published {} {} @ {} to {}-{}@{}",
                        event.exchange(), event.symbol(), event.price(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }

        } catch (InterruptedException ex) {
            // Never swallow: shutdown must be able to unwind the ingest executor.
            Thread.currentThread().interrupt();
            throw new MarketDataPublishException("Interrupted awaiting broker ack", ex);

        } catch (ExecutionException ex) {
            throw new MarketDataPublishException(
                    "Broker rejected event " + event.eventId(), ex.getCause() == null ? ex : ex.getCause());

        } catch (TimeoutException ex) {
            throw new MarketDataPublishException(
                    "Broker ack timed out after " + properties.producer().sendTimeout() + " for event "
                            + event.eventId(), ex);
        }
    }

    /**
     * Sends a batch and reports how many, from the front, the broker acknowledged.
     *
     * <p>For the spill drainer. A backlog replayed one blocking send at a time would crawl at
     * 1 / ack-latency, so the records are handed to the producer together and it batches
     * them. Only the leading run of successes is reported: the caller commits exactly that
     * many and retries from the first failure, which keeps a symbol's events in order.
     *
     * <p>No Retry or CircuitBreaker here on purpose. The drainer probes the broker through
     * {@link #publishOrThrow} first, so the breaker still sees the outcome, and its own loop
     * is the retry.
     */
    public int publishBatch(List<CanonicalTradeEvent> events) {
        List<CompletableFuture<SendResult<String, CanonicalTradeEvent>>> futures = new ArrayList<>(events.size());
        try {
            for (CanonicalTradeEvent event : events) {
                futures.add(kafkaTemplate.send(properties.topics().normalized(), event.partitionKey(), event));
            }
        } catch (RuntimeException ex) {
            // send() itself can throw (metadata timeout, buffer full). What was queued before
            // it still counts if it is acknowledged below.
            log.warn("Spill drain: send failed after queuing {} of {}: {}", futures.size(), events.size(), ex.toString());
        }
        int acknowledged = 0;
        for (CompletableFuture<SendResult<String, CanonicalTradeEvent>> future : futures) {
            try {
                future.get(properties.producer().sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
                acknowledged++;
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException | TimeoutException ex) {
                break;
            }
        }
        publishedCounter.increment(acknowledged);
        return acknowledged;
    }

    /**
     * Resilience4j fallback: retries are spent or the breaker is open.
     *
     * <p>It only reports the failure. What happens to the event is the caller's decision —
     * the ingestion path spills it to disk so nothing is lost while the broker is away.
     * Package-private rather than private so the intent is greppable; Resilience4j resolves
     * it reflectively either way. The signature must mirror {@link #publish} — including its
     * return type — with a trailing {@link Throwable}.
     */
    @SuppressWarnings("unused")
    boolean publishFallback(CanonicalTradeEvent event, Throwable throwable) {
        fallbackCounter.increment();
        // WARN, not ERROR: with the spill buffer this is a delay, not a loss, and during an
        // outage it fires for every event until the breaker opens.
        log.warn("Publish failed for event {} ({} {}): {}",
                event.eventId(), event.exchange(), event.symbol(), throwable.toString());
        return false;
    }
}
