package com.mdg.gateway.support;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A local stand-in for the three venues' historical-trades REST endpoints.
 *
 * <p>It reproduces how each one <em>pages</em>, which is the part worth testing: Binance
 * forwards from an id, Coinbase backwards from an id, Kraken forwards from a timestamp with a
 * cursor. Every trade id from 1 to {@link #setLatestTradeId} exists unless listed in
 * {@link #removeTrades}; trade {@code n} executed at {@code EPOCH + n} seconds.
 */
public final class FakeVenueRestServer implements AutoCloseable {

    public static final long EPOCH_SECONDS = 1_700_000_000L;

    private final HttpServer server;
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final Set<Long> removed = ConcurrentHashMap.newKeySet();
    private final AtomicInteger failAfterRequests = new AtomicInteger(Integer.MAX_VALUE);

    private volatile long latestTradeId = 10_000_000L;
    private volatile int pageSize = 1000;
    private volatile int forcedStatus;
    private volatile String retryAfter;

    public FakeVenueRestServer() {
        try {
            server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
        server.createContext("/api/v3/historicalTrades", exchange -> respond(exchange, this::binance));
        server.createContext("/products/", exchange -> respond(exchange, this::coinbase));
        server.createContext("/0/public/Trades", exchange -> respond(exchange, this::kraken));
        server.start();
    }

    public URI url() {
        return URI.create("http://localhost:" + server.getAddress().getPort());
    }

    /** Requests received so far, as path + query. */
    public List<String> requests() {
        return List.copyOf(requests);
    }

    public void setLatestTradeId(long latestTradeId) {
        this.latestTradeId = latestTradeId;
    }

    /** Venue-side maximum rows per page (the real ones cap at 1000). */
    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /** Ids that simply do not exist at the venue. */
    public void removeTrades(Long... ids) {
        removed.addAll(List.of(ids));
    }

    /** Answer every request with this status (e.g. 429), optionally with a Retry-After. */
    public void failWith(int status, String retryAfterHeader) {
        this.retryAfter = retryAfterHeader;
        this.forcedStatus = status;
    }

    /** Serve this many more requests normally, then answer 500. */
    public void failAfter(int requests) {
        failAfterRequests.set(this.requests.size() + requests);
    }

    public void reset() {
        requests.clear();
        removed.clear();
        failAfterRequests.set(Integer.MAX_VALUE);
        latestTradeId = 10_000_000L;
        pageSize = 1000;
        forcedStatus = 0;
        retryAfter = null;
    }

    @Override
    public void close() {
        server.stop(0);
    }

    // ------------------------------------------------------------------

    private interface Endpoint {
        String handle(String path, Map<String, String> query);
    }

    private void respond(HttpExchange exchange, Endpoint endpoint) throws IOException {
        URI uri = exchange.getRequestURI();
        requests.add(uri.getPath() + "?" + uri.getRawQuery());
        int status = 200;
        String body;
        if (forcedStatus != 0) {
            status = forcedStatus;
            body = "{\"msg\":\"slow down\"}";
            if (retryAfter != null) {
                exchange.getResponseHeaders().add("Retry-After", retryAfter);
            }
        } else if (requests.size() > failAfterRequests.get()) {
            status = 500;
            body = "{\"msg\":\"internal error\"}";
        } else {
            body = endpoint.handle(uri.getPath(), query(uri));
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (var out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static Map<String, String> query(URI uri) {
        Map<String, String> query = new HashMap<>();
        if (uri.getRawQuery() != null) {
            for (String pair : uri.getRawQuery().split("&")) {
                String[] parts = pair.split("=", 2);
                query.put(parts[0], parts.length > 1 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "");
            }
        }
        return query;
    }

    private boolean exists(long id) {
        return id >= 1 && id <= latestTradeId && !removed.contains(id);
    }

    /** Oldest first, starting at fromId. */
    private String binance(String path, Map<String, String> query) {
        long from = Long.parseLong(query.get("fromId"));
        int limit = Math.min(pageSize, Integer.parseInt(query.getOrDefault("limit", "500")));
        List<String> rows = new ArrayList<>();
        for (long id = from; id <= latestTradeId && rows.size() < limit; id++) {
            if (exists(id)) {
                rows.add("{\"id\":" + id + ",\"price\":\"82858.80000000\",\"qty\":\"0.00076000\","
                        + "\"quoteQty\":\"62.97268800\",\"time\":" + (EPOCH_SECONDS + id) * 1000
                        + ",\"isBuyerMaker\":false,\"isBestMatch\":true}");
            }
        }
        return "[" + String.join(",", rows) + "]";
    }

    /** Newest first, ids strictly below `after`. */
    private String coinbase(String path, Map<String, String> query) {
        long after = Long.parseLong(query.get("after"));
        int limit = Math.min(pageSize, Integer.parseInt(query.getOrDefault("limit", "1000")));
        List<String> rows = new ArrayList<>();
        for (long id = Math.min(after - 1, latestTradeId); id >= 1 && rows.size() < limit; id--) {
            if (exists(id)) {
                rows.add("{\"trade_id\":" + id + ",\"side\":\"sell\",\"size\":\"0.00005729\","
                        + "\"price\":\"82771.00000000\",\"time\":\""
                        + java.time.Instant.ofEpochSecond(EPOCH_SECONDS + id) + "\"}");
            }
        }
        return "[" + String.join(",", rows) + "]";
    }

    /** Oldest first, executed strictly after `since` (nanoseconds), with a `last` cursor. */
    private String kraken(String path, Map<String, String> query) {
        long sinceSeconds = Long.parseLong(query.get("since")) / 1_000_000_000L;
        // Trade n executed at exactly EPOCH + n seconds, so "after `since`" starts at the next id.
        long first = Math.max(1, sinceSeconds - EPOCH_SECONDS + 1);
        List<String> rows = new ArrayList<>();
        long lastId = first - 1;
        for (long id = first; id <= latestTradeId && rows.size() < pageSize; id++) {
            lastId = id;
            if (exists(id)) {
                rows.add("[\"82781.20000\",\"0.00005275\"," + (EPOCH_SECONDS + id) + ".000000,\"b\",\"l\",\"\"," + id + "]");
            }
        }
        return "{\"error\":[],\"result\":{\"" + query.get("pair") + "\":[" + String.join(",", rows)
                + "],\"last\":\"" + (EPOCH_SECONDS + lastId) + "000000000\"}}";
    }
}
