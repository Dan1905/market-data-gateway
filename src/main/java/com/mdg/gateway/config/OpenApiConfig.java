package com.mdg.gateway.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * OpenAPI description of the gateway's <em>control plane</em>.
 *
 * <h2>What this does and does not cover</h2>
 * This is a small surface on purpose. The gateway is middleware: its product is the stream
 * of {@link com.mdg.gateway.model.CanonicalTradeEvent} on {@code normalized-market-data},
 * not an HTTP API. Everything documented here is operational — the DLQ replay endpoint an
 * operator calls after shipping a fix, plus the actuator endpoints.
 *
 * <p>OpenAPI cannot describe the part that matters. It is a specification for synchronous
 * request/response APIs, and this system's contract is asynchronous: a message schema, a
 * topic, and a set of DLQ headers. That contract is documented in AsyncAPI instead — see
 * {@code docs/asyncapi.yaml}, served at {@code /asyncapi.html}. Anyone integrating with
 * this gateway needs that document, not this one.
 */
@Configuration(proxyBeanMethods = false)
public class OpenApiConfig {

    @Bean
    public OpenAPI marketDataGatewayOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Market Data Gateway — Control Plane")
                        .version("v1")
                        .description("""
                                Operational HTTP API for the market data integration gateway.

                                **This is not the gateway's main interface.** The gateway ingests trade \
                                and ticker feeds from Binance, Coinbase and Kraken over WebSockets, \
                                normalizes them into a single canonical schema, and publishes to Kafka. \
                                Consumers integrate with the **topics**, not with HTTP.

                                The event contract — the `CanonicalTradeEvent` schema, the \
                                `normalized-market-data` topic, and the dead letter envelope with its \
                                `X-Exception-Message` / `X-Source-Exchange` / `X-Failure-Stage` headers \
                                — is specified in AsyncAPI at [`/asyncapi.html`](/asyncapi.html).

                                What lives here:

                                * `POST /api/v1/dlq/replay` — re-run dead-lettered frames through the \
                                  pipeline after fixing whatever rejected them.
                                * `/actuator/*` — health (including per-venue connection state), \
                                  Prometheus metrics, and Resilience4j circuit breaker state.

                                ⚠️ **Unauthenticated.** The replay endpoint republishes to a production \
                                topic. Do not expose this service to the internet without an \
                                authenticating reverse proxy — see `deploy/` in the repository.
                                """)
                        .contact(new Contact().name("Market Data Platform"))
                        .license(new License().name("Proprietary")))
                .externalDocs(new ExternalDocumentation()
                        .description("AsyncAPI — the Kafka event contract (the one consumers need)")
                        .url("/asyncapi.html"))
                .servers(List.of(
                        new Server().url("/").description("This gateway")));
    }
}
