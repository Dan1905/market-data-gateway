package com.mdg.gateway.backfill;

import com.fasterxml.jackson.databind.JsonNode;
import com.mdg.gateway.dto.CoinbaseTradePayload;
import com.mdg.gateway.mapper.ExchangePayloadMapper;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.util.List;

/**
 * {@code GET /products/BTC-USD/trades?after=N&limit=1000} on the Coinbase Exchange API.
 *
 * <p>The Advanced Trade socket and the Exchange REST API share one trade-id space, which is
 * what makes this possible. {@code after=N} returns trades with an id <em>below</em>
 * {@code N}, newest first, so a gap is paged backwards from its end.
 */
public final class CoinbaseBackfillClient implements TradeBackfillClient {

    private static final int PAGE = 1000;

    private final URI baseUrl;
    private final BackfillHttp http;
    private final ExchangePayloadMapper mapper;

    public CoinbaseBackfillClient(URI baseUrl, BackfillHttp http, ExchangePayloadMapper mapper) {
        this.baseUrl = baseUrl;
        this.http = http;
        this.mapper = mapper;
    }

    @Override
    public Exchange exchange() {
        return Exchange.COINBASE;
    }

    @Override
    public void fetch(String venueSymbol, long fromId, long toId, Instant lastSeenAt, List<CanonicalTradeEvent> into) {
        long before = toId + 1;   // exclusive upper bound of the next page
        while (before > fromId) {
            long limit = Math.min(PAGE, before - fromId);
            JsonNode page = http.get(URI.create(baseUrl + "/products/" + venueSymbol
                    + "/trades?after=" + before + "&limit=" + limit));
            long lowest = Long.MAX_VALUE;
            for (JsonNode row : page) {
                long id = row.path("trade_id").asLong();
                lowest = Math.min(lowest, id);
                if (id < fromId || id > toId) {
                    continue;
                }
                CoinbaseTradePayload payload = new CoinbaseTradePayload(
                        venueSymbol, Long.toString(id),
                        // REST pads to 8 decimals ("82771.00000000"); the socket does not.
                        // Same number either way, but consumers should see one spelling.
                        new BigDecimal(row.path("price").asText()).stripTrailingZeros(),
                        new BigDecimal(row.path("size").asText()).stripTrailingZeros(),
                        Instant.parse(row.path("time").asText()),
                        row.path("side").asText(null));
                into.add(mapper.fromCoinbase(payload, true, row.toString()));
            }
            if (lowest >= before) {
                return;   // empty page, or no progress
            }
            before = lowest;
        }
    }
}
