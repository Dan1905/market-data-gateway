package com.mdg.gateway.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.ExecutorService;

/**
 * The shared outbound {@link HttpClient} used for every venue WebSocket.
 *
 * <p>One client for all venues: it owns a selector thread and a connection pool, and three
 * of those on a 1 GB box is pure overhead.
 *
 * <p>Handing it the virtual-thread executor means the JDK delivers WebSocket listener
 * callbacks on virtual threads rather than on its internal pool — so even the frame
 * reassembly in {@code AbstractExchangeWebSocketClient} runs off the selector thread.
 */
@Configuration(proxyBeanMethods = false)
public class HttpClientConfig {

    @Bean
    public HttpClient marketDataHttpClient(
            @Qualifier(VirtualThreadConfig.INGESTION_EXECUTOR) ExecutorService ingestionExecutor) {

        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1) // RFC 6455 upgrade is an HTTP/1.1 handshake
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .executor(ingestionExecutor)
                .build();
    }
}
