package com.mdg.gateway.venue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.mdg.gateway.exception.PayloadParsingException;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The parts of frame parsing that are the same for every venue: reading JSON, turning every
 * way a frame can be wrong into the pipeline's one terminal exception, and putting a batch
 * into trade order.
 */
@Component
public class FrameReader {

    private final ObjectMapper objectMapper;
    private final ObjectReader exactDecimals;

    public FrameReader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.exactDecimals = objectMapper.reader().with(JsonNodeFactory.withExactBigDecimals(true));
    }

    public <T> T read(Exchange exchange, String rawFrame, Class<T> type) {
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

    /** Parses a frame once into a tree, so a batch's individual trades can be cut out of it. */
    public JsonNode tree(Exchange exchange, String rawFrame) {
        try {
            // Exact decimals: by default the tree model strips trailing zeros (4136.40 becomes
            // 4136.4), and a "raw" payload that has been tidied is not raw.
            JsonNode root = exactDecimals.readTree(rawFrame);
            if (root == null || root.isMissingNode() || root.isNull()) {
                throw new PayloadParsingException(exchange, rawFrame, "Frame deserialized to null");
            }
            return root;
        } catch (JsonProcessingException ex) {
            throw new PayloadParsingException(exchange, rawFrame,
                    "Malformed " + exchange + " JSON: " + ex.getOriginalMessage(), ex);
        }
    }

    /** Binds an already-parsed frame to its typed envelope. */
    public <T> T read(Exchange exchange, String rawFrame, JsonNode root, Class<T> type) {
        try {
            T value = objectMapper.treeToValue(root, type);
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
     * One trade's own JSON, cut out of a batch frame, to be the event's {@code rawPayload}.
     *
     * <p>A batch frame used to be copied whole onto every event it produced: a Coinbase
     * snapshot of 100 trades put the same 20 KB on the topic 100 times. Each event now
     * carries only the object that describes it. Written with the gateway's own mapper, so
     * decimals keep the venue's digits: no exponent, and trailing zeros intact (see {@link #tree}).
     */
    public String slice(JsonNode tradeNode) {
        try {
            return objectMapper.writeValueAsString(tradeNode);
        } catch (JsonProcessingException ex) {
            // Re-serializing a tree that was just parsed cannot realistically fail.
            throw new IllegalStateException("Could not re-serialize a trade from its frame", ex);
        }
    }

    /**
     * Runs a MapStruct mapping, translating the canonical model's validation failures into
     * the pipeline's terminal exception so callers have exactly one thing to catch.
     */
    public CanonicalTradeEvent map(Exchange exchange, String rawFrame, MappingAttempt attempt) {
        try {
            return attempt.get();
        } catch (IllegalArgumentException | NullPointerException | ArithmeticException ex) {
            throw new PayloadParsingException(exchange, rawFrame,
                    "Invalid " + exchange + " payload: " + ex.getMessage(), ex);
        }
    }

    @FunctionalInterface
    public interface MappingAttempt {
        CanonicalTradeEvent get();
    }

    /**
     * Orders a batch by venue symbol, then ascending trade id — the order the venue executed
     * them. Downstream ordering guarantees (lanes, one partition per symbol) only preserve the
     * order events are emitted in, so the batch has to start out right.
     */
    public static List<CanonicalTradeEvent> inTradeOrder(List<CanonicalTradeEvent> events) {
        if (events.size() < 2) {
            return List.copyOf(events);
        }
        List<CanonicalTradeEvent> sorted = new ArrayList<>(events);
        sorted.sort(Comparator.comparing(CanonicalTradeEvent::venueSymbol)
                .thenComparing(CanonicalTradeEvent::tradeId, FrameReader::compareTradeIds));
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
