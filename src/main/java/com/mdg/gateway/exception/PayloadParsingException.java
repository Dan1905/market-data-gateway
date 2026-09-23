package com.mdg.gateway.exception;

import com.mdg.gateway.model.Exchange;

/**
 * Raised when a raw venue frame cannot be turned into a {@link com.mdg.gateway.model.CanonicalTradeEvent}.
 *
 * <p>This is a <em>terminal</em> failure: malformed JSON or a missing mandatory field is
 * deterministic, so retrying it only burns CPU and delays the DLQ write. Everything that
 * throws this goes straight to {@code market-data-dlq} with the raw frame attached.
 */
public class PayloadParsingException extends RuntimeException {

    private final Exchange exchange;
    private final String rawPayload;

    /** May be {@code null} when the frame arrived with no identifiable source venue. */
    public Exchange getExchange() {
        return exchange;
    }

    /** The verbatim frame, carried through to the DLQ record body. */
    public String getRawPayload() {
        return rawPayload;
    }

    public PayloadParsingException(Exchange exchange, String rawPayload, String message, Throwable cause) {
        super(message, cause);
        this.exchange = exchange;
        this.rawPayload = rawPayload;
    }

    public PayloadParsingException(Exchange exchange, String rawPayload, String message) {
        this(exchange, rawPayload, message, null);
    }
}
