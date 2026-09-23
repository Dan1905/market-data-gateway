package com.mdg.gateway.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Java 21 threading model for the ingest path.
 *
 * <p>{@code spring.threads.virtual.enabled=true} covers Tomcat request handling and
 * {@code @Async}, but the WebSocket ingest path never touches either — frames arrive on
 * the JDK {@code HttpClient}'s own selector threads. Those threads must not be blocked, so
 * this config supplies the executor that frame handling is handed off to.
 *
 * <p>The payoff is concrete: {@code MarketDataProducer} parks on the Kafka broker ack
 * (a genuinely blocking {@code Future.get}) so the circuit breaker can observe real
 * failures, and with a virtual thread that park costs a continuation mount rather than an
 * OS thread. A platform-thread pool would need to be sized for peak concurrent sends; this
 * one does not need sizing at all.
 */
@Configuration(proxyBeanMethods = false)
public class VirtualThreadConfig {

    private static final Logger log = LoggerFactory.getLogger(VirtualThreadConfig.class);

    public static final String INGESTION_EXECUTOR = "ingestionExecutor";
    public static final String RECONNECT_SCHEDULER = "reconnectScheduler";

    /**
     * Unbounded virtual-thread executor for decoding and publishing a single frame.
     *
     * <p>Unbounded is a deliberate choice, not an oversight: backpressure on this pipeline
     * belongs at ingest (the Resilience4j rate limiter) and at the broker (the producer's
     * own buffer), not in a queue that would silently grow heap on a 1 GB box.
     */
    @Bean(name = INGESTION_EXECUTOR, destroyMethod = "close")
    public ExecutorService ingestionExecutor() {
        ThreadFactory factory = Thread.ofVirtual().name("mdg-ingest-", 0).factory();
        log.info("Ingestion executor: virtual-thread-per-task");
        return Executors.newThreadPerTaskExecutor(factory);
    }

    /**
     * Timers for reconnect backoff and idle-socket detection.
     *
     * <p>Platform threads on purpose: {@code ScheduledExecutorService} owns its worker
     * threads and cannot be backed by virtual ones. It only ever *schedules* — the work it
     * fires is immediately handed to {@link #ingestionExecutor()} — so two threads is
     * plenty even with every venue reconnecting at once.
     */
    @Bean(name = RECONNECT_SCHEDULER, destroyMethod = "shutdownNow")
    public ScheduledExecutorService reconnectScheduler() {
        return Executors.newScheduledThreadPool(2, runnable -> {
            Thread thread = new Thread(runnable, "mdg-reconnect");
            thread.setDaemon(true);
            return thread;
        });
    }
}
