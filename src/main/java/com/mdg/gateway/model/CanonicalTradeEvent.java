package com.mdg.gateway.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

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
 * <h2>Identity</h2>
 * {@code eventId} is <em>derived</em>, not random: it is a name-based UUID of
 * {@code exchange + venueSymbol + tradeId}. The same trade therefore always carries the same
 * id — whether it arrives live, again after a reconnect, in a venue snapshot, from a REST
 * backfill or from a DLQ replay. That is what makes deduplication possible both inside the
 * gateway (see {@code TradeDeduplicator}) and, beyond its window, for any consumer.
 *
 * <p>The id is keyed on the <b>venue</b> symbol, not the normalized one, on purpose. With
 * stablecoin collapsing enabled, Binance {@code BTCUSDT} and {@code BTCUSDC} both normalize
 * to {@code BTC-USD}, but each has its own independent trade-id sequence. Keying on the
 * normalized symbol would make two different trades collide whenever their numeric ids
 * coincided, and the second would be silently dropped as a "duplicate".
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public record CanonicalTradeEvent(

        /* Deterministic identity: nameUUID(exchange:venueSymbol:tradeId). The dedup key. */
        String eventId,

        /* Venue name, e.g. "BINANCE". Kept as String on the wire for schema stability. */
        String exchange,

        /* Normalized instrument, e.g. "BTC-USD". See SymbolNormalizer. */
        String symbol,

        /* Instrument exactly as the venue names it, e.g. "BTCUSDT", "BTC/USD". */
        String venueSymbol,

        /* The venue's own trade id, sequential per venue symbol. */
        String tradeId,

        BigDecimal price,

        BigDecimal quantity,

        /* Venue execution time. */
        Instant timestamp,

        /*
         * True when the trade did not arrive on the live stream in real time: a venue snapshot
         * sent on (re)subscribe, or a REST backfill of a gap. Such events can be minutes old
         * and arrive after newer ones, so consumers that care about ordering should sort by
         * tradeId rather than trust arrival order.
         */
        boolean backfilled,

        /* Verbatim source frame, retained for audit and DLQ replay. */
        String rawPayload) {

    private static final String ID_NAMESPACE = "mdg:trade:";

    public CanonicalTradeEvent {
        requireText(eventId, "eventId");
        requireText(exchange, "exchange");
        requireText(symbol, "symbol");
        requireText(venueSymbol, "venueSymbol");
        requireText(tradeId, "tradeId");

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

    /**
     * The deterministic event id for a venue trade.
     *
     * @return a name-based (type 3) UUID string, or {@code null} if any part is missing — in
     *         which case the constructor's {@code eventId is required} check rejects the event,
     *         keeping the "why was this dead-lettered" message in one place
     */
    public static String deterministicEventId(String exchange, String venueSymbol, String tradeId) {
        if (isBlank(exchange) || isBlank(venueSymbol) || isBlank(tradeId)) {
            return null;
        }
        String name = ID_NAMESPACE + exchange + ':' + venueSymbol + ':' + tradeId;
        return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private static void requireText(String value, String field) {
        if (isBlank(value)) {
            throw new IllegalArgumentException(field + " is required");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
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
                .venueSymbol(venueSymbol)
                .tradeId(tradeId)
                .price(price)
                .quantity(quantity)
                .timestamp(timestamp)
                .backfilled(backfilled)
                .rawPayload(rawPayload);
    }

    /**
     * Fluent builder. {@link #build()} delegates to the canonical constructor, so every
     * invariant above applies to builder-constructed instances too — there is no way to
     * assemble an invalid event. MapStruct's generated mapper uses this builder too.
     */
    public static final class Builder {

        private String eventId;
        private String exchange;
        private String symbol;
        private String venueSymbol;
        private String tradeId;
        private BigDecimal price;
        private BigDecimal quantity;
        private Instant timestamp;
        private boolean backfilled;
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

        public Builder venueSymbol(String venueSymbol) {
            this.venueSymbol = venueSymbol;
            return this;
        }

        public Builder tradeId(String tradeId) {
            this.tradeId = tradeId;
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

        public Builder backfilled(boolean backfilled) {
            this.backfilled = backfilled;
            return this;
        }

        public Builder rawPayload(String rawPayload) {
            this.rawPayload = rawPayload;
            return this;
        }

        public CanonicalTradeEvent build() {
            return new CanonicalTradeEvent(eventId, exchange, symbol, venueSymbol, tradeId,
                    price, quantity, timestamp, backfilled, rawPayload);
        }
    }
}
