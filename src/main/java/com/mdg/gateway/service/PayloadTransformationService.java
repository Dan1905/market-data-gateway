package com.mdg.gateway.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.dto.BinanceTradePayload;
import com.mdg.gateway.dto.CoinbaseMarketTradesEnvelope;
import com.mdg.gateway.dto.CoinbaseTradePayload;
import com.mdg.gateway.dto.KrakenMessageEnvelope;
import com.mdg.gateway.dto.KrakenTradePayload;
import com.mdg.gateway.exception.PayloadParsingException;
import com.mdg.gateway.mapper.ExchangePayloadMapper;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Turns one raw venue frame into zero or more canonical events.
 *
 * <h2>Zero is a normal outcome, and this is the important part</h2>
 * All three venues multiplex control traffic onto the same socket as market data:
 * Binance answers subscriptions with {@code {"result":null,"id":1}}, Kraken emits
 * {@code channel:"heartbeat"} and {@code channel:"status"} frames continuously, Coinbase
 * sends a {@code subscriptions} confirmation. None of these are trades and none of them
 * are <em>failures</em>. A naive implementation dead-letters them, and within an hour the
 * DLQ is thousands of heartbeats deep and the one genuinely corrupt frame is invisible.
 * So control frames return an empty list and are counted, never dead-lettered.
 *
 * <h2>What does fail</h2>
 * Malformed JSON, or a trade frame that cannot satisfy {@link CanonicalTradeEvent}'s
 * invariants (null price, missing timestamp, negative quantity). Both are deterministic,
 * so both throw {@link PayloadParsingException} and go straight to the DLQ without retry.
 */
@Service
public class PayloadTransformationService {

    private static final Logger log = LoggerFactory.getLogger(PayloadTransformationService.class);

    private final ObjectMapper objectMapper;
    private final ExchangePayloadMapper payloadMapper;

    public PayloadTransformationService(ObjectMapper objectMapper, ExchangePayloadMapper payloadMapper) {
        this.objectMapper = objectMapper;
        this.payloadMapper = payloadMapper;
    }

    /**
     * @return canonical events, possibly empty for control frames
     * @throws PayloadParsingException if the frame is malformed or violates canonical invariants
     */
    public List<CanonicalTradeEvent> transform(Exchange exchange, String rawFrame) {
        if (exchange == null) {
            throw new PayloadParsingException(null, rawFrame, "Cannot transform a frame with no source exchange");
        }
        if (rawFrame == null || rawFrame.isBlank()) {
            throw new PayloadParsingException(exchange, rawFrame, "Empty frame");
        }

        return switch (exchange) {
            case BINANCE -> transformBinance(rawFrame);
            case COINBASE -> transformCoinbase(rawFrame);
            case KRAKEN -> transformKraken(rawFrame);
        };
    }

    // ------------------------------------------------------------------

    private List<CanonicalTradeEvent> transformBinance(String rawFrame) {
        BinanceTradePayload payload = read(Exchange.BINANCE, rawFrame, BinanceTradePayload.class);

        if (!payload.isTrade()) {
            log.debug("Skipping non-trade Binance frame (eventType={})", payload.eventType());
            return List.of();
        }
        // Binance has no snapshot on subscribe: every trade frame is live.
        return List.of(map(Exchange.BINANCE, rawFrame, () -> payloadMapper.fromBinance(payload, false, rawFrame)));
    }

    private List<CanonicalTradeEvent> transformCoinbase(String rawFrame) {
        CoinbaseMarketTradesEnvelope envelope =
                read(Exchange.COINBASE, rawFrame, CoinbaseMarketTradesEnvelope.class);

        if (!envelope.isMarketTrades()) {
            log.debug("Skipping non-trade Coinbase frame (channel={})", envelope.channel());
            return List.of();
        }

        // Coinbase batches N trades per frame. One bad entry fails the whole frame: the frame
        // is the unit stored in the DLQ, so partial success would make a replay duplicate the
        // entries that already succeeded (deduplication would catch it, but only inside its
        // window).
        List<CanonicalTradeEvent> events = new ArrayList<>();
        for (CoinbaseMarketTradesEnvelope.Event event : envelope.safeEvents()) {
            if (event == null || event.trades() == null) {
                continue;
            }
            // The snapshot sent on subscribe replays recent trades — possibly ones already
            // published before a reconnect. Flag them; dedup drops the overlap.
            boolean backfilled = event.isSnapshot();
            for (CoinbaseTradePayload trade : event.trades()) {
                if (trade == null) {
                    continue;
                }
                events.add(map(Exchange.COINBASE, rawFrame,
                        () -> payloadMapper.fromCoinbase(trade, backfilled, rawFrame)));
            }
        }
        if (events.isEmpty()) {
            log.debug("Coinbase market_trades frame carried no trades");
        }
        // Coinbase lists a batch newest-first. Published as-is, a consumer would see trade
        // 935 before 934 on the same symbol.
        return inTradeOrder(events);
    }

    private List<CanonicalTradeEvent> transformKraken(String rawFrame) {
        KrakenMessageEnvelope envelope = read(Exchange.KRAKEN, rawFrame, KrakenMessageEnvelope.class);

        if (envelope.error() != null && !envelope.error().isBlank()) {
            // A venue-side rejection of *our* subscription is an operational problem, not a
            // corrupt payload — surface it loudly but do not dead-letter it.
            log.error("Kraken returned an error frame: {}", envelope.error());
            return List.of();
        }
        if (!envelope.isTrade()) {
            log.debug("Skipping non-trade Kraken frame (channel={}, method={})",
                    envelope.channel(), envelope.method());
            return List.of();
        }

        // "snapshot" frames replay recent trades on subscribe; "update" frames are live.
        boolean backfilled = envelope.isSnapshot();
        List<CanonicalTradeEvent> events = new ArrayList<>();
        for (KrakenTradePayload trade : envelope.safeData()) {
            if (trade == null) {
                continue;
            }
            events.add(map(Exchange.KRAKEN, rawFrame,
                    () -> payloadMapper.fromKraken(trade, backfilled, rawFrame)));
        }
        return inTradeOrder(events);
    }

    // ------------------------------------------------------------------

    private <T> T read(Exchange exchange, String rawFrame, Class<T> type) {
        try {
            T value = objectMapper.readValue(rawFrame, type);
            if (value == null) {
                throw new PayloadParsingException(exchange, rawFrame, "Frame deserialized to null");
            }
            return value;
        } catch (JsonProcessingException ex) {
            throw new PayloadParsingException(exchange, rawFrame,
                    "Malformed " + exchange + " JSON: " + ex.getOriginalMessage(), ex);
        }
    }

    /**
     * Runs a MapStruct mapping, translating the canonical model's validation failures into
     * the pipeline's terminal exception so callers have exactly one thing to catch.
     */
    private CanonicalTradeEvent map(Exchange exchange, String rawFrame, MappingAttempt attempt) {
        try {
            return attempt.get();
        } catch (IllegalArgumentException | NullPointerException | ArithmeticException ex) {
            throw new PayloadParsingException(exchange, rawFrame,
                    "Invalid " + exchange + " payload: " + ex.getMessage(), ex);
        }
    }

    @FunctionalInterface
    private interface MappingAttempt {
        CanonicalTradeEvent get();
    }

    /**
     * Orders a batch by venue symbol, then ascending trade id — the order the venue executed
     * them. Downstream ordering guarantees (lanes, one partition per symbol) only preserve the
     * order events are emitted in, so the batch has to start out right.
     */
    static List<CanonicalTradeEvent> inTradeOrder(List<CanonicalTradeEvent> events) {
        if (events.size() < 2) {
            return List.copyOf(events);
        }
        List<CanonicalTradeEvent> sorted = new ArrayList<>(events);
        sorted.sort(Comparator.comparing(CanonicalTradeEvent::venueSymbol)
                .thenComparing(CanonicalTradeEvent::tradeId, PayloadTransformationService::compareTradeIds));
        return List.copyOf(sorted);
    }

    /**
     * Numeric trade ids compare by value without parsing (they can outgrow a {@code long} in
     * principle): a shorter digit string is smaller, equal lengths compare lexically. Anything
     * non-numeric falls back to plain string order.
     */
    static int compareTradeIds(String a, String b) {
        if (isDigits(a) && isDigits(b) && a.length() != b.length()) {
            return Integer.compare(a.length(), b.length());
        }
        return a.compareTo(b);
    }

    private static boolean isDigits(String s) {
        return !s.isEmpty() && s.chars().allMatch(c -> c >= '0' && c <= '9');
    }
}
