package com.mdg.gateway.service;

import com.mdg.gateway.exception.PayloadParsingException;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.venue.Venue;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

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
 *
 * <p>How each venue's frames are read lives with that venue (see {@link Venue#parse}). This
 * class only validates the input and routes it, so it has no per-venue code to extend.
 */
@Service
public class PayloadTransformationService {

    private final Map<Exchange, Venue> venues = new EnumMap<>(Exchange.class);

    public PayloadTransformationService(List<Venue> venues) {
        venues.forEach(venue -> this.venues.put(venue.exchange(), venue));
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
        Venue venue = venues.get(exchange);
        if (venue == null) {
            throw new PayloadParsingException(exchange, rawFrame, "No venue is registered for " + exchange);
        }
        return venue.parse(rawFrame);
    }
}
