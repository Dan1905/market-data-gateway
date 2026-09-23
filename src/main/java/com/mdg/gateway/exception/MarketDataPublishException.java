package com.mdg.gateway.exception;

/**
 * Raised when the broker could not accept a canonical event.
 *
 * <p>Unlike {@link PayloadParsingException} this is usually <em>transient</em> (leader
 * election, broker restart, network blip), which is exactly why the publish path — not the
 * parse path — is the one wrapped in Resilience4j Retry + CircuitBreaker.
 */
public class MarketDataPublishException extends RuntimeException {

    public MarketDataPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
