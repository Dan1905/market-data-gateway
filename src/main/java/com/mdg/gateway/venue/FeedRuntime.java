package com.mdg.gateway.venue;

import com.mdg.gateway.service.MarketDataIngestionService;
import io.micrometer.core.instrument.MeterRegistry;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The shared machinery every socket client runs on, bundled so that {@link Venue#createClient}
 * does not grow a parameter each time the clients need one more collaborator.
 *
 * @param ingestionService  where frames go
 * @param ingestionExecutor virtual threads for frame handling and paced sends
 * @param scheduler         reconnects, the idle watchdog, the stability timer
 * @param httpClient        the one outbound client shared by all venues
 */
public record FeedRuntime(MarketDataIngestionService ingestionService,
                          ExecutorService ingestionExecutor,
                          ScheduledExecutorService scheduler,
                          HttpClient httpClient,
                          MeterRegistry meterRegistry) {
}
