package com.mdg.gateway.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.config.GatewayProperties;
import com.mdg.gateway.config.JacksonConfig;
import com.mdg.gateway.exception.MarketDataPublishException;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.support.Fixtures;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SpillBufferTest {

    @TempDir
    Path dir;

    private final ObjectMapper mapper = JacksonConfig.marketDataObjectMapper();
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final MarketDataProducer producer = mock(MarketDataProducer.class);

    /** What the fake broker has accepted, in order. */
    private final List<String> delivered = Collections.synchronizedList(new ArrayList<>());
    private final AtomicBoolean brokerUp = new AtomicBoolean(false);
    private final List<SpillBuffer> buffers = new ArrayList<>();

    @AfterEach
    void stopBuffers() {
        buffers.forEach(SpillBuffer::stop);
    }

    private SpillBuffer buffer(boolean enabled, long maxBytes) {
        GatewayProperties base = Fixtures.gatewayProperties();
        GatewayProperties props = new GatewayProperties(base.topics(), base.symbol(), base.producer(), base.dlq(),
                base.dedup(), base.lanes(),
                new GatewayProperties.Spill(enabled, dir, maxBytes, 1 << 16, false, 3, Duration.ofMillis(50)));

        // A broker that is either down (everything fails) or up (everything is acknowledged).
        doAnswer(invocation -> {
            if (!brokerUp.get()) {
                throw new MarketDataPublishException("broker down", null);
            }
            delivered.add(((CanonicalTradeEvent) invocation.getArgument(0)).tradeId());
            return null;
        }).when(producer).publishOrThrow(any());
        when(producer.publishBatch(anyList())).thenAnswer(invocation -> {
            List<CanonicalTradeEvent> events = invocation.getArgument(0);
            if (!brokerUp.get()) {
                return 0;
            }
            events.forEach(event -> delivered.add(event.tradeId()));
            return events.size();
        });

        SpillBuffer buffer = new SpillBuffer(props, producer, mapper, meterRegistry);
        buffers.add(buffer);
        return buffer;
    }

    private static CanonicalTradeEvent trade(int id) {
        return Fixtures.canonicalEvent().toBuilder()
                .tradeId(Integer.toString(id))
                .eventId(CanonicalTradeEvent.deterministicEventId("BINANCE", "BTCUSDT", Integer.toString(id)))
                .build();
    }

    @Test
    @DisplayName("holds events while the broker is down, then delivers them all in order")
    void holdsThenDrainsInOrder() {
        SpillBuffer buffer = buffer(true, 1 << 20);
        buffer.start();
        assertThat(buffer.isActive()).isFalse();

        for (int i = 1; i <= 10; i++) {
            assertThat(buffer.offer(trade(i))).isTrue();
        }
        // Active is what tells the ingestion path to queue behind these rather than publish.
        assertThat(buffer.isActive()).isTrue();
        assertThat(buffer.pending()).isEqualTo(10);
        assertThat(delivered).isEmpty();

        brokerUp.set(true);

        await().atMost(Duration.ofSeconds(10)).until(() -> !buffer.isActive());
        assertThat(delivered).containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
        assertThat(meterRegistry.counter("mdg.spill.appended").count()).isEqualTo(10.0);
        assertThat(meterRegistry.counter("mdg.spill.drained").count()).isEqualTo(10.0);
        assertThat(buffer.bytes()).isZero();
    }

    @Test
    @DisplayName("round-trips an event through disk without losing precision or fields")
    void eventSurvivesTheRoundTrip() {
        CanonicalTradeEvent original = trade(42).toBuilder()
                .price(new java.math.BigDecimal("0.00000005"))
                .backfilled(true)
                .build();
        List<CanonicalTradeEvent> sent = new ArrayList<>();
        SpillBuffer buffer = buffer(true, 1 << 20);
        doAnswer(invocation -> sent.add(invocation.getArgument(0))).when(producer).publishOrThrow(any());

        buffer.start();
        buffer.offer(original);

        await().atMost(Duration.ofSeconds(10)).until(() -> !buffer.isActive());
        assertThat(sent).containsExactly(original);
    }

    @Test
    @DisplayName("events left on disk at shutdown are published by the next start")
    void survivesRestart() {
        SpillBuffer first = buffer(true, 1 << 20);
        first.start();
        for (int i = 1; i <= 5; i++) {
            first.offer(trade(i));
        }
        first.stop();
        assertThat(delivered).isEmpty();

        brokerUp.set(true);
        SpillBuffer second = buffer(true, 1 << 20);
        second.start();
        // Active from the first instant, so nothing new can jump the queue.
        await().atMost(Duration.ofSeconds(10)).until(() -> !second.isActive());

        assertThat(delivered).containsExactly("1", "2", "3", "4", "5");
    }

    @Test
    @DisplayName("refuses when full, so the caller can dead-letter instead of blocking forever")
    void refusesWhenFull() {
        SpillBuffer buffer = buffer(true, 3_000);
        buffer.start();

        int accepted = 0;
        while (buffer.offer(trade(accepted + 1))) {
            accepted++;
        }

        assertThat(accepted).isGreaterThan(0);
        assertThat(buffer.bytes()).isLessThanOrEqualTo(3_000);
        assertThat(meterRegistry.counter("mdg.spill.rejected").count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("when disabled it takes nothing and is never active")
    void disabled() {
        SpillBuffer buffer = buffer(false, 1 << 20);
        buffer.start();

        assertThat(buffer.offer(trade(1))).isFalse();
        assertThat(buffer.isActive()).isFalse();
        assertThat(buffer.isRunning()).isTrue();
    }

    @Test
    @DisplayName("an unwritable directory degrades to 'refuse' instead of stopping the gateway")
    void unusableDirectory() throws Exception {
        Path file = java.nio.file.Files.createFile(dir.resolve("not-a-directory"));
        GatewayProperties base = Fixtures.gatewayProperties();
        SpillBuffer buffer = new SpillBuffer(new GatewayProperties(base.topics(), base.symbol(), base.producer(),
                base.dlq(), base.dedup(), base.lanes(),
                new GatewayProperties.Spill(true, file.resolve("spill"), 1 << 20, 1 << 16, false, 3,
                        Duration.ofMillis(50))), producer, mapper, meterRegistry);
        buffers.add(buffer);

        buffer.start();

        assertThat(buffer.isRunning()).isTrue();
        assertThat(buffer.offer(trade(1))).isFalse();
    }
}
