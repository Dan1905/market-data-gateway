package com.mdg.gateway.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * The unified wire contract published to {@code normalized-market-data}.
 *
 * <p>This record is the single point where heterogeneous venue schemas collapse into one
 * shape, so its invariants are enforced in the compact constructor rather than left to
 * downstream consumers. Any raw payload that cannot satisfy them is — by design — a DLQ
 * candidate: it is better to reject a malformed tick loudly at the edge than to publish a
 * null price into a topic that trading systems read.
 *
 * <p>A record because immutability here is a hard requirement: the same instance is handed
 * to the Kafka producer, the audit path, and metrics from multiple virtual threads.
 *
 * <p>The builder is written out by hand rather than generated. With seven components — four
 * of which are {@code String} and two {@code BigDecimal} — positional construction is easy
 * to get silently wrong, so a builder earns its place; but see the note on the build tooling
 * in {@code pom.xml} for why this project does not use an annotation processor to produce it.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public record CanonicalTradeEvent(

        /* Gateway-assigned identity. Not the venue trade id — venues reuse and collide. */
        String eventId,

        /* Venue name, e.g. "BINANCE". Kept as String on the wire for schema stability. */
        String exchange,

        /* Normalized instrument, e.g. "BTC-USD". See SymbolNormalizer. */
        String symbol,

        BigDecimal price,

        BigDecimal quantity,

        /* Venue event time where available, gateway receive time otherwise. */
        Instant timestamp,

        /* Verbatim source frame, retained for audit and DLQ replay. */
        String rawPayload) {

    public CanonicalTradeEvent {
        requireText(eventId, "eventId");
        requireText(exchange, "exchange");
        requireText(symbol, "symbol");

        if (price == null) {
            throw new IllegalArgumentException("price is required");
        }
        if (price.signum() <= 0) {
            throw new IllegalArgumentException("price must be positive but was " + price.toPlainString());
        }
        if (quantity == null) {
            throw new IllegalArgumentException("quantity is required");
        }
        if (quantity.signum() < 0) {
            throw new IllegalArgumentException("quantity must not be negative but was " + quantity.toPlainString());
        }
        if (timestamp == null) {
            throw new IllegalArgumentException("timestamp is required");
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
    }

    /**
     * Kafka partition key. Keying by {@code symbol} (not exchange) keeps all venues for one
     * instrument on the same partition, which is what consumers doing cross-venue
     * comparison need for ordering.
     */
    public String partitionKey() {
        return symbol;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Returns a builder pre-populated from this event, for deriving a modified copy. */
    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .exchange(exchange)
                .symbol(symbol)
                .price(price)
                .quantity(quantity)
                .timestamp(timestamp)
                .rawPayload(rawPayload);
    }

    /**
     * Fluent builder. {@link #build()} delegates to the canonical constructor, so every
     * invariant above applies to builder-constructed instances too — there is no way to
     * assemble an invalid event.
     */
    public static final class Builder {

        private String eventId;
        private String exchange;
        private String symbol;
        private BigDecimal price;
        private BigDecimal quantity;
        private Instant timestamp;
        private String rawPayload;

        private Builder() {
        }

        public Builder eventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder exchange(String exchange) {
            this.exchange = exchange;
            return this;
        }

        public Builder symbol(String symbol) {
            this.symbol = symbol;
            return this;
        }

        public Builder price(BigDecimal price) {
            this.price = price;
            return this;
        }

        public Builder quantity(BigDecimal quantity) {
            this.quantity = quantity;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder rawPayload(String rawPayload) {
            this.rawPayload = rawPayload;
            return this;
        }

        public CanonicalTradeEvent build() {
            return new CanonicalTradeEvent(
                    eventId, exchange, symbol, price, quantity, timestamp, rawPayload);
        }
    }
}
