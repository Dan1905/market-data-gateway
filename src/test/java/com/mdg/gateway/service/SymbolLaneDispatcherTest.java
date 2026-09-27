package com.mdg.gateway.service;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class SymbolLaneDispatcherTest {

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    private SymbolLaneDispatcher dispatcher(int maxInFlight) {
        return new SymbolLaneDispatcher(executor, maxInFlight, Duration.ofSeconds(5), meterRegistry);
    }

    @Test
    @DisplayName("keeps order within a lane while running lanes concurrently")
    void orderedWithinLaneConcurrentAcross() {
        SymbolLaneDispatcher lanes = dispatcher(10_000);
        int laneCount = 32;
        int perLane = 300;
        Map<String, List<Integer>> seen = new ConcurrentHashMap<>();
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxRunning = new AtomicInteger();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Interleave submissions the way a multi-symbol socket does: one frame per symbol, round-robin.
        for (int i = 0; i < perLane; i++) {
            for (int lane = 0; lane < laneCount; lane++) {
                String key = "BINANCE:SYM" + lane;
                int seq = i;
                futures.add(lanes.submit(key, () -> {
                    maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
                    // Random jitter so a lane that is not truly serialized would visibly reorder.
                    LockSupport.parkNanos(ThreadLocalRandom.current().nextInt(20_000));
                    seen.computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>())).add(seq);
                    running.decrementAndGet();
                }));
            }
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();

        List<Integer> expected = java.util.stream.IntStream.range(0, perLane).boxed().toList();
        assertThat(seen).hasSize(laneCount);
        seen.values().forEach(order -> assertThat(order).containsExactlyElementsOf(expected));
        // Concurrency across lanes is the point of the change, so it is asserted, not assumed.
        assertThat(maxRunning.get()).isGreaterThan(1);
        assertThat(lanes.inFlightCount()).isZero();
        assertThat(meterRegistry.get("mdg.lanes.active").gauge().value()).isEqualTo(laneCount);
    }

    @Test
    @DisplayName("never runs two tasks of the same lane at once")
    void laneIsMutuallyExclusive() {
        SymbolLaneDispatcher lanes = dispatcher(10_000);
        AtomicInteger concurrent = new AtomicInteger();
        AtomicBoolean overlapped = new AtomicBoolean();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            futures.add(lanes.submit("KRAKEN:BTC/USD", () -> {
                if (concurrent.incrementAndGet() > 1) {
                    overlapped.set(true);
                }
                Thread.yield();
                concurrent.decrementAndGet();
            }));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        assertThat(overlapped).isFalse();
    }

    @Test
    @DisplayName("a failing task does not stall the tasks queued behind it")
    void failureDoesNotStallLane() {
        SymbolLaneDispatcher lanes = dispatcher(100);
        List<Integer> ran = Collections.synchronizedList(new ArrayList<>());

        lanes.submit("L", () -> ran.add(1));
        CompletableFuture<Void> failing = lanes.submit("L", () -> {
            throw new IllegalStateException("boom");
        });
        CompletableFuture<Void> after = lanes.submit("L", () -> ran.add(3));

        after.join();
        assertThat(ran).containsExactly(1, 3);
        assertThatThrownBy(failing::join).hasRootCauseMessage("boom");
        assertThat(lanes.inFlightCount()).isZero();
    }

    @Test
    @DisplayName("blocks the submitter when the in-flight budget is spent, and resumes as it frees")
    void backpressureBlocksSubmitter() throws Exception {
        SymbolLaneDispatcher lanes = dispatcher(2);
        CountDownLatch gate = new CountDownLatch(1);
        Runnable parked = () -> {
            try {
                gate.await();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        };
        lanes.submit("A", parked);
        lanes.submit("B", parked);
        assertThat(lanes.inFlightCount()).isEqualTo(2);

        // The third submission stands in for the socket reader: it must wait, not queue unboundedly.
        CompletableFuture<Void> third = CompletableFuture.runAsync(() -> lanes.submit("C", () -> { }), executor);
        TimeUnit.MILLISECONDS.sleep(200);
        assertThat(third).isNotDone();
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(meterRegistry.counter("mdg.lanes.backpressure.waits").count()).isEqualTo(1.0));

        gate.countDown();
        third.get(5, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(5)).until(() -> lanes.inFlightCount() == 0);
    }

    @Test
    @DisplayName("returns the permit when the executor rejects the task")
    void rejectedTaskReleasesPermit() {
        ExecutorService dead = Executors.newSingleThreadExecutor();
        dead.shutdown();
        SymbolLaneDispatcher lanes = new SymbolLaneDispatcher(dead, 1, Duration.ofSeconds(1), meterRegistry);

        CompletableFuture<Void> rejected = lanes.submit("L", () -> { });

        assertThat(rejected).isCompletedExceptionally();
        // With a budget of one, a leaked permit would make this second submit block forever.
        assertThat(lanes.inFlightCount()).isZero();
        assertThat(lanes.submit("L", () -> { })).isCompletedExceptionally();
    }

    @Test
    @DisplayName("stop() waits for queued work to finish before returning")
    void stopDrains() {
        SymbolLaneDispatcher lanes = dispatcher(100);
        lanes.start();
        AtomicInteger done = new AtomicInteger();
        for (int i = 0; i < 20; i++) {
            lanes.submit("L", () -> {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                done.incrementAndGet();
            });
        }

        lanes.stop();

        assertThat(done).hasValue(20);
        assertThat(lanes.isRunning()).isFalse();
    }

    @Test
    @DisplayName("stop() gives up after the drain timeout instead of hanging shutdown")
    void stopIsBounded() {
        SymbolLaneDispatcher lanes = new SymbolLaneDispatcher(executor, 10, Duration.ofMillis(200), meterRegistry);
        CountDownLatch never = new CountDownLatch(1);
        lanes.submit("L", () -> {
            try {
                never.await();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        });

        long start = System.nanoTime();
        lanes.stop();
        long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertThat(tookMs).isBetween(150L, 2_000L);
        assertThat(lanes.inFlightCount()).isEqualTo(1);
        never.countDown();
    }
}
