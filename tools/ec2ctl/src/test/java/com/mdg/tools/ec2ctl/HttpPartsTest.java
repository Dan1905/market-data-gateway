package com.mdg.tools.ec2ctl;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** DuckDNS and the health check against a local HTTP server: nothing leaves this machine. */
class HttpPartsTest {

    private final HttpClient http = HttpClient.newHttpClient();
    private final List<String> queries = new CopyOnWriteArrayList<>();
    private HttpServer server;

    private URI serve(String path, int status, String body) throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext(path, exchange -> {
            queries.add(exchange.getRequestURI().getRawQuery());
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        return URI.create("http://localhost:" + server.getAddress().getPort());
    }

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    @DisplayName("DuckDNS update sends domain, token and IP, and accepts OK")
    void duckDnsOk() throws IOException {
        DuckDns dns = new DuckDns(http, serve("/update", 200, "OK"));

        dns.update("example-gateway", "tok/en+1", "13.61.200.5");

        // The token is URL-encoded, so a token with / or + still reaches DuckDNS intact.
        assertThat(queries).containsExactly("domains=example-gateway&token=tok%2Fen%2B1&ip=13.61.200.5");
    }

    @Test
    @DisplayName("DuckDNS answering KO is an error that names the likely cause, not a silent success")
    void duckDnsKo() throws IOException {
        DuckDns dns = new DuckDns(http, serve("/update", 200, "KO"));

        assertThatThrownBy(() -> dns.update("example-gateway", "wrong", "13.61.200.5"))
                .hasMessageContaining("KO").hasMessageContaining("DUCKDNS_TOKEN");
    }

    @Test
    @DisplayName("health check keeps polling until the gateway answers 200")
    void healthWaitsForUp() throws IOException {
        AtomicInteger calls = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/live", exchange -> {
            int status = calls.incrementAndGet() < 3 ? 503 : 200;
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
        });
        server.start();
        URI url = URI.create("http://localhost:" + server.getAddress().getPort() + "/live");
        AtomicInteger sleeps = new AtomicInteger();

        boolean up = new HealthCheck(http, ms -> sleeps.incrementAndGet()).waitUntilUp(url, 10, Duration.ofSeconds(10));

        assertThat(up).isTrue();
        assertThat(calls).hasValue(3);
        assertThat(sleeps).hasValue(2);
    }

    @Test
    @DisplayName("health check gives up after its attempts")
    void healthGivesUp() throws IOException {
        URI base = serve("/live", 503, "");

        assertThat(new HealthCheck(http, ms -> { }).waitUntilUp(base.resolve("/live"), 3, Duration.ofSeconds(1))).isFalse();
    }
}
