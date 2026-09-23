package com.mdg.gateway.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.config.ExchangeProperties;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.service.MarketDataIngestionService;
import com.mdg.gateway.support.Fixtures;
import com.mdg.gateway.support.TestVenueServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

/**
 * Exercises the ingestion clients against a local WebSocket server.
 *
 * <p>This is the part of the gateway that cannot be reasoned about from unit tests alone:
 * demand-driven frame delivery, reassembly of fragmented messages, ordered hand-off to
 * virtual threads, connection loss, and reconnect with backoff. All of it runs here for
 * real against {@link TestVenueServer} — a real socket, a real handshake, real frames —
 * with no dependency on Binance, Coinbase or Kraken being reachable.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@Import(TestVenueServer.class)
class ExchangeWebSocketClientTest {

    @MockitoBean
    private KafkaAdmin kafkaAdmin;

    /** The pipeline entry point is stubbed: this test is about the socket, not normalization. */
    @MockitoBean
    private MarketDataIngestionService ingestionService;

    @Autowired
    private TestVenueServer.VenueHandler venue;

    @LocalServerPort
    private int port;

    private static final ExecutorService INGEST_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();
    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "test-reconnect");
        t.setDaemon(true);
        return t;
    });
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .executor(INGEST_EXECUTOR)
            .build();

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final List<String> ingested = new ArrayList<>();
    private final List<ExchangeWebSocketClient> started = new ArrayList<>();

    @BeforeEach
    void setUp() throws IOException {
        venue.reset();
        ingested.clear();
        doAnswer(invocation -> {
            synchronized (ingested) {
                ingested.add(invocation.getArgument(1));
            }
            return null;
        }).when(ingestionService).ingest(any(), any());
    }

    @AfterEach
    void tearDown() {
        started.forEach(ExchangeWebSocketClient::close);
        started.clear();
    }

    @AfterAll
    static void shutdownExecutors() {
        SCHEDULER.shutdownNow();
        INGEST_EXECUTOR.close();
    }

    // ------------------------------------------------------------------

    @Test
    @DisplayName("connects, and forwards each frame to the ingestion pipeline")
    void forwardsFramesToIngestion() throws Exception {
        BinanceWebSocketClient client = startBinance(connection(Duration.ofSeconds(30)));

        awaitConnected(client);
        venue.send(Fixtures.BINANCE_TRADE);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(ingested).containsExactly(Fixtures.BINANCE_TRADE));

        verify(ingestionService).ingest(eq(Exchange.BINANCE), eq(Fixtures.BINANCE_TRADE));
    }

    @Test
    @DisplayName("reassembles a frame split across continuation frames")
    void reassemblesFragmentedFrames() throws Exception {
        BinanceWebSocketClient client = startBinance(connection(Duration.ofSeconds(30)));
        awaitConnected(client);

        // The JDK client delivers this as several onText callbacks with last=false. If the
        // buffer logic were wrong we would ingest fragments of JSON and dead-letter them all.
        venue.sendFragmented(Fixtures.BINANCE_TRADE, 5);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(ingested).containsExactly(Fixtures.BINANCE_TRADE));
    }

    @Test
    @DisplayName("preserves frame order — the reason onText returns a CompletionStage")
    void preservesFrameOrder() throws Exception {
        BinanceWebSocketClient client = startBinance(connection(Duration.ofSeconds(30)));
        awaitConnected(client);

        List<String> sent = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            String frame = "{\"e\":\"trade\",\"seq\":" + i + "}";
            sent.add(frame);
            venue.send(frame);
        }

        // Dispatching fire-and-forget would be faster and would reorder these. For market
        // data that is the wrong trade-off, so ordering is asserted explicitly.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(ingested).containsExactlyElementsOf(sent));
    }

    @Test
    @DisplayName("Kraken sends its subscribe frame on connect")
    void krakenSubscribesOnConnect() throws Exception {
        KrakenWebSocketClient client = new KrakenWebSocketClient(
                connection(Duration.ofSeconds(30), "BTC/USD"), ingestionService,
                INGEST_EXECUTOR, SCHEDULER, HTTP_CLIENT, new ObjectMapper(), meterRegistry);
        start(client);
        awaitConnected(client);

        String subscribe = venue.received().poll(10, TimeUnit.SECONDS);
        assertThat(subscribe).isNotNull();
        assertThat(subscribe)
                .contains("\"method\":\"subscribe\"")
                .contains("\"channel\":\"trade\"")
                .contains("\"BTC/USD\"")
                // snapshot:false — otherwise every reconnect replays stale trades as new events.
                .contains("\"snapshot\":false");
    }

    @Test
    @DisplayName("Coinbase subscribes to heartbeats and ticker, one frame per channel")
    void coinbaseSubscribesPerChannel() throws Exception {
        CoinbaseWebSocketClient client = new CoinbaseWebSocketClient(
                connection(Duration.ofSeconds(30), "BTC-USD"), ingestionService,
                INGEST_EXECUTOR, SCHEDULER, HTTP_CLIENT, new ObjectMapper(), meterRegistry);
        start(client);
        awaitConnected(client);

        List<String> frames = new ArrayList<>();
        frames.add(venue.received().poll(10, TimeUnit.SECONDS));
        frames.add(venue.received().poll(10, TimeUnit.SECONDS));

        assertThat(frames).noneMatch(java.util.Objects::isNull);
        assertThat(frames).anySatisfy(frame -> assertThat(frame).contains("\"channel\":\"heartbeats\""));
        assertThat(frames).anySatisfy(frame -> assertThat(frame).contains("\"channel\":\"ticker\""));
        assertThat(frames).allSatisfy(frame -> assertThat(frame).contains("\"BTC-USD\""));
    }

    @Test
    @DisplayName("Binance sends no subscribe frame — its stream is encoded in the URL")
    void binanceSendsNoSubscription() throws Exception {
        BinanceWebSocketClient client = startBinance(connection(Duration.ofSeconds(30)));
        awaitConnected(client);
        venue.send(Fixtures.BINANCE_TRADE);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(ingested).isNotEmpty());
        assertThat(venue.received()).isEmpty();
    }

    @Test
    @DisplayName("reconnects after the venue hangs up, and keeps ingesting")
    void reconnectsAfterVenueDisconnect() throws Exception {
        BinanceWebSocketClient client = startBinance(connection(Duration.ofSeconds(30)));
        awaitConnected(client);
        assertThat(venue.connectionCount()).isEqualTo(1);

        venue.disconnect();

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(venue.connectionCount()).isGreaterThanOrEqualTo(2));
        awaitConnected(client);

        // The feed must be genuinely usable again, not merely re-handshaken.
        venue.send(Fixtures.BINANCE_TRADE);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(ingested).contains(Fixtures.BINANCE_TRADE));
    }

    @Test
    @DisplayName("the idle watchdog recycles a socket that is open but silent")
    void idleWatchdogForcesReconnect() throws Exception {
        // A venue-side fault can leave the TCP connection open and completely silent. No
        // close frame, no error, no Ping to answer — the watchdog is the only thing that
        // notices, so it is worth proving it fires.
        BinanceWebSocketClient client = startBinance(connection(Duration.ofSeconds(2)));
        awaitConnected(client);
        assertThat(venue.connectionCount()).isEqualTo(1);

        await().atMost(Duration.ofSeconds(25))
                .untilAsserted(() -> assertThat(venue.connectionCount()).isGreaterThanOrEqualTo(2));
    }

    @Test
    @DisplayName("close() disarms reconnection")
    void closeStopsReconnecting() throws Exception {
        BinanceWebSocketClient client = startBinance(connection(Duration.ofSeconds(30)));
        awaitConnected(client);

        client.close();
        started.remove(client);

        int countAtClose = venue.connectionCount();
        // A client that kept reconnecting after shutdown would hold the JVM open and keep
        // hammering the venue after a deploy.
        Thread.sleep(3_000);
        assertThat(venue.connectionCount()).isEqualTo(countAtClose);
        assertThat(client.isConnected()).isFalse();
    }

    @Test
    @DisplayName("start() and close() are idempotent")
    void lifecycleIsIdempotent() throws Exception {
        BinanceWebSocketClient client = startBinance(connection(Duration.ofSeconds(30)));
        awaitConnected(client);

        client.start();   // second start must not open a second socket
        Thread.sleep(500);
        assertThat(venue.connectionCount()).isEqualTo(1);

        client.close();
        client.close();   // second close must not throw
        started.remove(client);
        assertThat(client.isConnected()).isFalse();
    }

    @Test
    @DisplayName("a failed connect counts toward backoff and keeps retrying")
    void failedConnectsAccumulateBackoff() {
        ExchangeProperties.Connection unreachable = new ExchangeProperties.Connection(
                true,
                // Reserved TEST-NET-1 address: guaranteed not to answer, so the connect
                // attempt fails rather than reaching something unexpected.
                URI.create("ws://192.0.2.1:9/venue"),
                List.of("BTCUSDT"),
                new ExchangeProperties.Backoff(
                        Duration.ofMillis(100), Duration.ofMillis(300), 2.0, 0.0),
                Duration.ofMillis(300),
                Duration.ofSeconds(30),
                1_048_576);

        BinanceWebSocketClient client = startBinance(unreachable);

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(client.consecutiveFailures()).isGreaterThanOrEqualTo(2));
        assertThat(client.isConnected()).isFalse();
    }

    @Test
    @DisplayName("an oversized fragmented frame is dropped and the socket recycled")
    void oversizedFrameIsRejected() throws Exception {
        ExchangeProperties.Connection tinyLimit = new ExchangeProperties.Connection(
                true, venueUri(), List.of("BTCUSDT"),
                new ExchangeProperties.Backoff(
                        Duration.ofMillis(100), Duration.ofSeconds(1), 2.0, 0.0),
                Duration.ofSeconds(10), Duration.ofSeconds(30),
                64);   // bytes

        BinanceWebSocketClient client = startBinance(tinyLimit);
        awaitConnected(client);

        // Without the cap, a peer streaming an unbounded fragmented message would grow the
        // heap until the JVM dies.
        try {
            venue.sendFragmented("x".repeat(4_096), 16);
        } catch (IOException expected) {
            // "Broken pipe": the client aborted the socket partway through the stream,
            // which is precisely the behaviour under test. The server noticing is success.
        }

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(venue.connectionCount()).isGreaterThanOrEqualTo(2));
        assertThat(ingested).isEmpty();
    }

    // ------------------------------------------------------------------

    private BinanceWebSocketClient startBinance(ExchangeProperties.Connection config) {
        BinanceWebSocketClient client = new BinanceWebSocketClient(
                config, ingestionService, INGEST_EXECUTOR, SCHEDULER, HTTP_CLIENT, meterRegistry);
        return start(client);
    }

    private <T extends ExchangeWebSocketClient> T start(T client) {
        started.add(client);
        client.start();
        return client;
    }

    private void awaitConnected(ExchangeWebSocketClient client) {
        await().atMost(Duration.ofSeconds(15)).until(client::isConnected);
    }

    private ExchangeProperties.Connection connection(Duration idleTimeout, String... symbols) {
        return new ExchangeProperties.Connection(
                true,
                venueUri(),
                symbols.length == 0 ? List.of("BTCUSDT") : List.of(symbols),
                new ExchangeProperties.Backoff(
                        Duration.ofMillis(100), Duration.ofSeconds(1), 2.0, 0.0),
                Duration.ofSeconds(10),
                idleTimeout,
                1_048_576);
    }

    private URI venueUri() {
        return URI.create("ws://localhost:" + port + TestVenueServer.PATH);
    }
}
