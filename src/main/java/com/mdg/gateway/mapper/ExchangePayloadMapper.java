package com.mdg.gateway.mapper;

import com.mdg.gateway.dto.BinanceTradePayload;
import com.mdg.gateway.dto.CoinbaseTickerPayload;
import com.mdg.gateway.dto.KrakenTradePayload;
import com.mdg.gateway.model.CanonicalTradeEvent;
import org.mapstruct.InjectionStrategy;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.Named;
import org.mapstruct.ReportingPolicy;

import java.time.Instant;
import java.util.UUID;

/**
 * Compile-time normalization from three venue schemas onto {@link CanonicalTradeEvent}.
 *
 * <p>MapStruct rather than hand-written mapping or reflection for two reasons that matter
 * on a hot ingest path: the generated code is plain field assignment (no reflection, no
 * megamorphic call sites, JIT-friendly), and {@code unmappedTargetPolicy = ERROR} turns
 * "someone added a canonical field and forgot a venue" into a <em>build</em> failure rather
 * than a null in production.
 *
 * <p>Each method takes the verbatim frame as a second parameter so the audit trail in
 * {@code rawPayload} is exactly the bytes that arrived — reserializing the DTO would lose
 * whatever fields we chose not to model.
 */
@Mapper(
        componentModel = MappingConstants.ComponentModel.SPRING,
        uses = SymbolNormalizer.class,
        injectionStrategy = InjectionStrategy.CONSTRUCTOR,
        unmappedTargetPolicy = ReportingPolicy.ERROR,
        unmappedSourcePolicy = ReportingPolicy.IGNORE)
public interface ExchangePayloadMapper {

    // ------------------------------------------------------------------
    // Binance: concatenated symbol, string numerics, epoch-millis time
    // ------------------------------------------------------------------

    @Mapping(target = "eventId", expression = "java(newEventId())")
    @Mapping(target = "exchange", constant = "BINANCE")
    @Mapping(target = "symbol", source = "payload.symbol", qualifiedByName = "normalizeConcatenated")
    @Mapping(target = "price", source = "payload.price")
    @Mapping(target = "quantity", source = "payload.quantity")
    @Mapping(target = "timestamp", source = "payload.tradeTime", qualifiedByName = "epochMillisToInstant")
    @Mapping(target = "rawPayload", source = "rawPayload")
    CanonicalTradeEvent fromBinance(BinanceTradePayload payload, String rawPayload);

    // ------------------------------------------------------------------
    // Coinbase: hyphenated symbol, ISO-8601 time, 24h volume as quantity
    // ------------------------------------------------------------------

    @Mapping(target = "eventId", expression = "java(newEventId())")
    @Mapping(target = "exchange", constant = "COINBASE")
    @Mapping(target = "symbol", source = "payload.productId", qualifiedByName = "normalizeDelimited")
    @Mapping(target = "price", source = "payload.price")
    @Mapping(target = "quantity", source = "payload.volume24h")
    @Mapping(target = "timestamp", source = "payload.timestamp")
    @Mapping(target = "rawPayload", source = "rawPayload")
    CanonicalTradeEvent fromCoinbase(CoinbaseTickerPayload payload, String rawPayload);

    // ------------------------------------------------------------------
    // Kraken v2: slashed symbol, numeric price/qty, ISO-8601 time
    // ------------------------------------------------------------------

    @Mapping(target = "eventId", expression = "java(newEventId())")
    @Mapping(target = "exchange", constant = "KRAKEN")
    @Mapping(target = "symbol", source = "payload.symbol", qualifiedByName = "normalizeDelimited")
    @Mapping(target = "price", source = "payload.price")
    @Mapping(target = "quantity", source = "payload.quantity")
    @Mapping(target = "timestamp", source = "payload.timestamp")
    @Mapping(target = "rawPayload", source = "rawPayload")
    CanonicalTradeEvent fromKraken(KrakenTradePayload payload, String rawPayload);

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /**
     * Gateway-assigned event identity. Deliberately not derived from the venue trade id:
     * ids are only unique per venue per symbol, and Coinbase tickers have none at all.
     */
    default String newEventId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Null-safe epoch-millis conversion. Returning {@code null} for a missing timestamp is
     * intentional — {@link CanonicalTradeEvent}'s constructor is the single place that
     * decides a frame is unusable, which keeps the DLQ reason messages consistent.
     */
    @Named("epochMillisToInstant")
    default Instant epochMillisToInstant(Long epochMillis) {
        return epochMillis == null ? null : Instant.ofEpochMilli(epochMillis);
    }
}
