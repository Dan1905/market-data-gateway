package com.mdg.gateway.backfill;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.client.RetryAfter;
import io.github.resilience4j.ratelimiter.RateLimiter;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * One venue's REST access: every call waits for the venue's rate-limiter permit, and a
 * rate-limit response is surfaced as such rather than as a generic failure.
 */
public final class BackfillHttp {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Duration timeout;
    private final RateLimiter limiter;

    BackfillHttp(HttpClient httpClient, ObjectMapper objectMapper, Duration timeout, RateLimiter limiter) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.timeout = timeout;
        this.limiter = limiter;
    }

    JsonNode get(URI uri) {
        RateLimiter.waitForPermission(limiter);
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(timeout)
                .header("Accept", "application/json")
                // Coinbase rejects requests that carry no User-Agent.
                .header("User-Agent", "market-data-gateway")
                .GET()
                .build();
        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException ex) {
            throw new BackfillException("GET " + uri + " failed: " + ex, ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new BackfillException("Interrupted during GET " + uri, ex);
        }
        int status = response.statusCode();
        if (status == 429 || status == 418) {
            // 418 is Binance's "you kept going after 429 and are now banned for a while".
            throw BackfillException.rateLimited("GET " + uri + " was rate limited (HTTP " + status + ")",
                    response.headers().firstValue("Retry-After").map(RetryAfter::parse).orElse(null));
        }
        if (status / 100 != 2) {
            throw new BackfillException("GET " + uri + " returned HTTP " + status, null);
        }
        try {
            return objectMapper.readTree(response.body());
        } catch (IOException ex) {
            throw new BackfillException("GET " + uri + " returned unreadable JSON", ex);
        }
    }
}
