package com.mdg.tools.ec2ctl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Reads and updates the DuckDNS record that points at the instance.
 *
 * <p>DuckDNS's API takes the account token as a query parameter; that is its design, not a
 * choice made here. The request goes over HTTPS, and the URL is never printed or logged.
 */
class DuckDns {

    private final HttpClient http;
    private final URI api;

    DuckDns(HttpClient http, URI api) {
        this.http = http;
        this.api = api;
    }

    /** The IP the domain resolves to right now, or {@code null} if it does not resolve. */
    String resolve(String domain) {
        try {
            return InetAddress.getByName(domain + ".duckdns.org").getHostAddress();
        } catch (UnknownHostException ex) {
            return null;
        }
    }

    /** Points the domain at {@code ip}. DuckDNS answers a plain {@code OK} or {@code KO}. */
    void update(String domain, String token, String ip) {
        URI uri = URI.create(api + "/update?domains=" + enc(domain) + "&token=" + enc(token) + "&ip=" + enc(ip));
        try {
            HttpResponse<String> response = http.send(
                    HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(15)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200 || !"OK".equals(response.body().trim())) {
                // KO usually means a wrong token or a domain this account does not own.
                throw new IllegalStateException("DuckDNS refused the update (HTTP " + response.statusCode()
                        + ", body '" + response.body().trim() + "'). Check DUCKDNS_TOKEN and DUCKDNS_DOMAIN.");
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Could not reach DuckDNS: " + ex.getMessage(), ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while updating DuckDNS", ex);
        }
    }

    private static String enc(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
