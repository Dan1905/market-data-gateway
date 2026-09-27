package com.mdg.gateway.mapper;

import com.mdg.gateway.dto.BinanceTradePayload;
import com.mdg.gateway.dto.CoinbaseTradePayload;
import com.mdg.gateway.dto.KrakenTradePayload;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import org.mapstruct.InjectionStrategy;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.Named;
import org.mapstruct.ReportingPolicy;

import java.time.Instant;

/**
 * Compile-time normalization from three venue schemas onto {@link CanonicalTradeEvent}.
 *
 * <p>MapStruct rather than hand-written mapping or reflection for two reasons that matter
 * on a hot ingest path: the generated code is plain field assignment (no reflection, no
 * megamorphic call sites, JIT-friendly), and {@code unmappedTargetPolicy = ERROR} turns
 * "someone added a canonical field and forgot a venue" into a <em>build</em> failure rather
 * than a null in production.
 *
 * <p><b>The implementation is generated once and committed</b> as
 * {@code ExchangePayloadMapperImpl.java}; the processor does not run during the normal build.
 * After changing this interface, regenerate it — see that file's header.
 *
 * <p>Each method takes the verbatim frame so the audit trail in {@code rawPayload} is exactly
 * the bytes that arrived, and a {@code backfilled} flag so snapshot and REST-backfilled trades
 * are marked as not having arrived live.
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

    @Mapping(target = "eventId", expression = "java(binanceEventId(payload))")
    @Mapping(target = "exchange", constant = "BINANCE")
    @Mapping(target = "symbol", source = "payload.symbol", qualifiedByName = "normalizeConcatenated")
    @Mapping(target = "venueSymbol", source = "payload.symbol")
    @Mapping(target = "tradeId", source = "payload.tradeId")
    @Mapping(target = "price", source = "payload.price")
    @Mapping(target = "quantity", source = "payload.quantity")
    @Mapping(target = "timestamp", source = "payload.tradeTime", qualifiedByName = "epochMillisToInstant")
    @Mapping(target = "backfilled", source = "backfilled")
    @Mapping(target = "rawPayload", source = "rawPayload")
    CanonicalTradeEvent fromBinance(BinanceTradePayload payload, boolean backfilled, String rawPayload);

    // ------------------------------------------------------------------
    // Coinbase market_trades: hyphenated symbol, string numerics, ISO-8601 time
    // ------------------------------------------------------------------

    @Mapping(target = "eventId", expression = "java(coinbaseEventId(payload))")
    @Mapping(target = "exchange", constant = "COINBASE")
    @Mapping(target = "symbol", source = "payload.productId", qualifiedByName = "normalizeDelimited")
    @Mapping(target = "venueSymbol", source = "payload.productId")
    @Mapping(target = "tradeId", source = "payload.tradeId")
    @Mapping(target = "price", source = "payload.price")
    @Mapping(target = "quantity", source = "payload.size")
    @Mapping(target = "timestamp", source = "payload.time")
    @Mapping(target = "backfilled", source = "backfilled")
    @Mapping(target = "rawPayload", source = "rawPayload")
    CanonicalTradeEvent fromCoinbase(CoinbaseTradePayload payload, boolean backfilled, String rawPayload);

    // ------------------------------------------------------------------
    // Kraken v2: slashed symbol, numeric price/qty, ISO-8601 time
    // ------------------------------------------------------------------

    @Mapping(target = "eventId", expression = "java(krakenEventId(payload))")
    @Mapping(target = "exchange", constant = "KRAKEN")
    @Mapping(target = "symbol", source = "payload.symbol", qualifiedByName = "normalizeDelimited")
    @Mapping(target = "venueSymbol", source = "payload.symbol")
    @Mapping(target = "tradeId", source = "payload.tradeId")
    @Mapping(target = "price", source = "payload.price")
    @Mapping(target = "quantity", source = "payload.quantity")
    @Mapping(target = "timestamp", source = "payload.timestamp")
    @Mapping(target = "backfilled", source = "backfilled")
    @Mapping(target = "rawPayload", source = "rawPayload")
    CanonicalTradeEvent fromKraken(KrakenTradePayload payload, boolean backfilled, String rawPayload);

    // ------------------------------------------------------------------
    // Identity. Null-safe so a malformed payload is rejected by the canonical constructor's
    // "eventId is required" check rather than by an NPE here.
    // ------------------------------------------------------------------

    default String binanceEventId(BinanceTradePayload payload) {
        return payload == null ? null : CanonicalTradeEvent.deterministicEventId(
                Exchange.BINANCE.name(), payload.symbol(), idToString(payload.tradeId()));
    }

    default String coinbaseEventId(CoinbaseTradePayload payload) {
        return payload == null ? null : CanonicalTradeEvent.deterministicEventId(
                Exchange.COINBASE.name(), payload.productId(), payload.tradeId());
    }

    default String krakenEventId(KrakenTradePayload payload) {
        return payload == null ? null : CanonicalTradeEvent.deterministicEventId(
                Exchange.KRAKEN.name(), payload.symbol(), idToString(payload.tradeId()));
    }

    private static String idToString(Long id) {
        return id == null ? null : id.toString();
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
