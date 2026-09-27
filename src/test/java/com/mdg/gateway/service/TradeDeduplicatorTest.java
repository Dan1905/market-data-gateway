package com.mdg.gateway.service;

import com.mdg.gateway.config.GatewayProperties;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.support.Fixtures;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class TradeDeduplicatorTest {

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

    private TradeDeduplicator deduplicator(boolean enabled, int maxEntries, Duration window) {
        GatewayProperties base = Fixtures.gatewayProperties();
        GatewayProperties props = new GatewayProperties(base.topics(), base.symbol(), base.producer(),
                base.dlq(), new GatewayProperties.Dedup(enabled, maxEntries, window));
        return new TradeDeduplicator(props, meterRegistry);
    }

    private static CanonicalTradeEvent trade(String id) {
        return Fixtures.canonicalEvent().toBuilder()
                .tradeId(id)
                .eventId(CanonicalTradeEvent.deterministicEventId("BINANCE", "BTCUSDT", id))
                .build();
    }

    @Test
    void firstCopyIsClaimedLaterCopiesAreDuplicates() {
        TradeDeduplicator dedup = deduplicator(true, 1_000, Duration.ofMinutes(10));

        assertThat(dedup.claim(trade("1"))).isTrue();
        assertThat(dedup.claim(trade("1"))).isFalse();
        assertThat(dedup.claim(trade("2"))).isTrue();

        assertThat(meterRegistry.counter("mdg.events.deduplicated", "exchange", "BINANCE").count())
                .isEqualTo(1.0);
    }

    @Test
    void releasedClaimCanBeClaimedAgain() {
        TradeDeduplicator dedup = deduplicator(true, 1_000, Duration.ofMinutes(10));

        assertThat(dedup.claim(trade("1"))).isTrue();
        dedup.release(trade("1"));
        assertThat(dedup.claim(trade("1"))).isTrue();
    }

    @Test
    void isDuplicateIsReadOnly() {
        TradeDeduplicator dedup = deduplicator(true, 1_000, Duration.ofMinutes(10));

        assertThat(dedup.isDuplicate(trade("1"))).isFalse();
        // Peeking must not claim, or a dry-run replay would change what a real run does.
        assertThat(dedup.claim(trade("1"))).isTrue();
        assertThat(dedup.isDuplicate(trade("1"))).isTrue();
    }

    @Test
    @DisplayName("ids are forgotten after the window, so memory is bounded in time")
    void forgetsAfterTheWindow() {
        TradeDeduplicator dedup = deduplicator(true, 1_000, Duration.ofMillis(200));

        assertThat(dedup.claim(trade("1"))).isTrue();
        await().atMost(Duration.ofSeconds(5)).until(() -> dedup.claim(trade("1")));
    }

    @Test
    @DisplayName("the cache never grows past maxEntries, so memory is bounded in size")
    void boundedBySize() {
        TradeDeduplicator dedup = deduplicator(true, 100, Duration.ofHours(1));

        for (int i = 0; i < 10_000; i++) {
            dedup.claim(trade(Integer.toString(i)));
        }

        // Caffeine evicts asynchronously; the gauge settles at or under the bound.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(meterRegistry.get("mdg.dedup.cache.size").gauge().value())
                        .isLessThanOrEqualTo(100));
    }

    @Test
    void disabledLetsEverythingThrough() {
        TradeDeduplicator dedup = deduplicator(false, 1_000, Duration.ofMinutes(10));

        assertThat(dedup.claim(trade("1"))).isTrue();
        assertThat(dedup.claim(trade("1"))).isTrue();
        assertThat(dedup.isDuplicate(trade("1"))).isFalse();
    }

    @Test
    @DisplayName("under contention exactly one of many concurrent copies wins the claim")
    void claimIsAtomicUnderContention() throws Exception {
        TradeDeduplicator dedup = deduplicator(true, 1_000, Duration.ofMinutes(10));
        CanonicalTradeEvent same = trade("race");
        int threads = 64;
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger winners = new AtomicInteger();

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    if (dedup.claim(same)) {
                        winners.incrementAndGet();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
        }

        // A check-then-put would let several through; putIfAbsent lets exactly one.
        assertThat(winners.get()).isEqualTo(1);
    }
}
