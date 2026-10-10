package com.mdg.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.producer.MarketDataProducer;
import com.mdg.gateway.producer.SpillBuffer;
import com.mdg.gateway.service.MarketDataIngestionService;
import com.mdg.gateway.support.AbstractKafkaIT;
import com.mdg.gateway.support.Fixtures;
import com.mdg.gateway.support.KafkaTestConsumer;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The claim "no data loss when the broker goes away", tested by taking the broker away.
 *
 * <p>The broker is frozen with {@code docker pause} while trades keep arriving, then resumed.
 * Every trade must reach the topic, in order, with nothing dead-lettered.
 *
 * <p>Its own application context (and so its own spill directory and circuit breaker), with a
 * short breaker wait so recovery is observed in seconds rather than half a minute.
 */
@TestPropertySource(properties = "resilience4j.circuitbreaker.instances.kafka-producer.wait-duration-in-open-state=2s")
@DirtiesContext
class BrokerOutageIT extends AbstractKafkaIT {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private MarketDataIngestionService ingestionService;

    @Autowired
    private SpillBuffer spill;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private CircuitBreakerRegistry circuitBreakerRegistry;

    @AfterEach
    void neverLeaveTheBrokerFrozen() {
        // If an assertion failed mid-outage, every later IT would hang on a paused broker.
        try {
            resumeBroker();
        } catch (RuntimeException alreadyRunning) {
            // expected on the happy path
        }
    }

    @Test
    @DisplayName("trades arriving during a broker outage are kept on disk and delivered in order afterwards")
    void nothingIsLostAcrossABrokerOutage() throws Exception {
        List<String> sent = new ArrayList<>();
        double undeliverableBefore = meterRegistry.counter("mdg.events.undeliverable").count();

        try (KafkaTestConsumer consumer = tail(NORMALIZED_TOPIC)) {
            // 1. Healthy broker.
            ingest(20, sent);
            await().atMost(Duration.ofSeconds(20)).until(() -> published() >= 20);

            // 2. Broker frozen; the feed does not stop for it.
            pauseBroker();
            ingest(60, sent);

            // Sends time out, the breaker opens, and everything lands in the spill buffer.
            await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                    assertThat(meterRegistry.counter("mdg.spill.appended").count()).isEqualTo(60.0));
            assertThat(spill.isActive()).isTrue();
            assertThat(circuitBreakerRegistry.circuitBreaker(MarketDataProducer.RESILIENCE_INSTANCE)
                    .getMetrics().getNumberOfFailedCalls()).isPositive();

            // 3. Broker back. More trades arrive while the backlog is still draining; they
            //    must queue behind it, not overtake it.
            resumeBroker();
            ingest(20, sent);

            await().atMost(Duration.ofSeconds(60)).until(() -> !spill.isActive());

            // 4. Everything is on the topic.
            Set<String> distinctInTopicOrder = new LinkedHashSet<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (distinctInTopicOrder.size() < sent.size() && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.drainFor(Duration.ofMillis(500))) {
                    JsonNode event = objectMapper.readTree(record.value());
                    if ("BTCUSDT".equals(event.get("venueSymbol").asText())) {
                        distinctInTopicOrder.add(event.get("tradeId").asText());
                    }
                }
            }

            // No loss and no reordering: first appearances are exactly what was sent, in the
            // order it was sent. (A record the frozen broker had already received can appear
            // twice — at-least-once — which is why the comparison is on first appearances;
            // such a repeat carries the same eventId.)
            assertThat(new ArrayList<>(distinctInTopicOrder)).containsExactlyElementsOf(sent);
        }

        assertThat(meterRegistry.counter("mdg.spill.drained").count()).isEqualTo(
                meterRegistry.counter("mdg.spill.appended").count());
        // Nothing fell through to the dead letter queue.
        assertThat(meterRegistry.counter("mdg.events.undeliverable").count()).isEqualTo(undeliverableBefore);
        assertThat(meterRegistry.counter("mdg.spill.rejected").count()).isZero();
        assertThat(spill.bytes()).isZero();
    }

    private void ingest(int count, List<String> sent) {
        for (int i = 0; i < count; i++) {
            long tradeId = Fixtures.nextTradeId();
            sent.add(Long.toString(tradeId));
            ingestionService.ingest(Exchange.BINANCE, Fixtures.binanceTrade(tradeId));
        }
    }

    private double published() {
        return meterRegistry.counter("mdg.events.published").count();
    }
}
