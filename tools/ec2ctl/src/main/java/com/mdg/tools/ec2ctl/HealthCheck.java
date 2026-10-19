package com.mdg.tools.ec2ctl;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.function.LongConsumer;

/** Polls the gateway's public liveness URL, the one endpoint that needs no login. */
class HealthCheck {

    private final HttpClient http;
    private final LongConsumer sleeper;

    HealthCheck(HttpClient http, LongConsumer sleeper) {
        this.http = http;
        this.sleeper = sleeper;
    }

    /** @return true once the URL answers 200, false if it never did within the attempts */
    boolean waitUntilUp(URI url, int attempts, Duration interval) {
        for (int i = 0; i < attempts; i++) {
            try {
                HttpResponse<Void> response = http.send(
                        HttpRequest.newBuilder(url).timeout(Duration.ofSeconds(10)).GET().build(),
                        HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() == 200) {
                    return true;
                }
            } catch (java.io.IOException ex) {
                // Not up yet: DNS still old, the container still starting, Caddy fetching a cert.
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return false;
            }
            sleeper.accept(interval.toMillis());
        }
        return false;
    }
}
