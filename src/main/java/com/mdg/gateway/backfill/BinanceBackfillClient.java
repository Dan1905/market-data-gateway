package com.mdg.gateway.backfill;

import com.fasterxml.jackson.databind.JsonNode;
import com.mdg.gateway.dto.BinanceTradePayload;
import com.mdg.gateway.mapper.ExchangePayloadMapper;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.util.List;

/**
 * {@code GET /api/v3/historicalTrades?symbol=BTCUSDT&fromId=N&limit=1000}
 *
 * <p>Returns trades from id {@code N} upward, oldest first, so a gap is paged forwards. No API
 * key is needed. Each call costs request weight 25 against 6000 a minute.
 */
final class BinanceBackfillClient implements TradeBackfillClient {

    private static final int PAGE = 1000;

    private final URI baseUrl;
    private final BackfillHttp http;
    private final ExchangePayloadMapper mapper;

    BinanceBackfillClient(URI baseUrl, BackfillHttp http, ExchangePayloadMapper mapper) {
        this.baseUrl = baseUrl;
        this.http = http;
        this.mapper = mapper;
    }

    @Override
    public Exchange exchange() {
        return Exchange.BINANCE;
    }

    @Override
    public void fetch(String venueSymbol, long fromId, long toId, Instant lastSeenAt, List<CanonicalTradeEvent> into) {
        long next = fromId;
        while (next <= toId) {
            long limit = Math.min(PAGE, toId - next + 1);
            JsonNode page = http.get(URI.create(baseUrl + "/api/v3/historicalTrades?symbol=" + venueSymbol
                    + "&fromId=" + next + "&limit=" + limit));
            long highest = -1;
            for (JsonNode row : page) {
                long id = row.path("id").asLong();
                highest = Math.max(highest, id);
                if (id < fromId || id > toId) {
                    continue;
                }
                long time = row.path("time").asLong();
                BinanceTradePayload payload = new BinanceTradePayload(
                        BinanceTradePayload.EVENT_TYPE_TRADE, time, venueSymbol, id,
                        new BigDecimal(row.path("price").asText()), new BigDecimal(row.path("qty").asText()),
                        time, row.path("isBuyerMaker").asBoolean());
                into.add(mapper.fromBinance(payload, true, row.toString()));
            }
            if (highest < next) {
                return;   // nothing at or after `next`: the venue has no more to give
            }
            next = highest + 1;
        }
    }
}
