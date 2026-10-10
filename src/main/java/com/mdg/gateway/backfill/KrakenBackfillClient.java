package com.mdg.gateway.backfill;

import com.fasterxml.jackson.databind.JsonNode;
import com.mdg.gateway.dto.KrakenTradePayload;
import com.mdg.gateway.mapper.ExchangePayloadMapper;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * {@code GET /0/public/Trades?pair=BTC/USD&since=<nanoseconds>}
 *
 * <p>Kraken pages by <em>time</em>, not id: it returns up to 1000 trades executed after
 * {@code since}, oldest first, each row being
 * {@code [price, volume, time, side, type, misc, trade_id]}, plus a {@code last} cursor for
 * the next page. So the search starts just before the last trade we delivered and rows are
 * filtered by id.
 */
final class KrakenBackfillClient implements TradeBackfillClient {

    /** Start a little early: an equal timestamp must not hide the first missing trade. */
    private static final Duration OVERLAP = Duration.ofSeconds(2);
    private static final BigInteger NANOS_PER_SECOND = BigInteger.valueOf(1_000_000_000L);
    /** Far more pages than {@code maxGap} can need; purely a guard against a cursor that never advances. */
    private static final int MAX_PAGES = 200;

    private final URI baseUrl;
    private final BackfillHttp http;
    private final ExchangePayloadMapper mapper;

    KrakenBackfillClient(URI baseUrl, BackfillHttp http, ExchangePayloadMapper mapper) {
        this.baseUrl = baseUrl;
        this.http = http;
        this.mapper = mapper;
    }

    @Override
    public Exchange exchange() {
        return Exchange.KRAKEN;
    }

    @Override
    public void fetch(String venueSymbol, long fromId, long toId, Instant lastSeenAt, List<CanonicalTradeEvent> into) {
        if (lastSeenAt == null) {
            throw new BackfillException("Kraken backfill needs the time of the last delivered trade", null);
        }
        Instant start = lastSeenAt.minus(OVERLAP);
        String since = BigInteger.valueOf(start.getEpochSecond()).multiply(NANOS_PER_SECOND)
                .add(BigInteger.valueOf(start.getNano())).toString();
        String pair = URLEncoder.encode(venueSymbol, StandardCharsets.UTF_8);

        for (int pageNumber = 0; pageNumber < MAX_PAGES; pageNumber++) {
            JsonNode body = http.get(URI.create(baseUrl + "/0/public/Trades?pair=" + pair + "&since=" + since));
            if (body.path("error").size() > 0) {
                String error = body.path("error").toString();
                if (error.contains("Rate limit") || error.contains("Too many requests")) {
                    throw BackfillException.rateLimited("Kraken refused the backfill: " + error, null);
                }
                throw new BackfillException("Kraken refused the backfill: " + error, null);
            }
            JsonNode result = body.path("result");
            // The trades are under the pair's name, which Kraken may spell differently from
            // the request ("XXBTZUSD"); it is whichever field is not the cursor.
            JsonNode rows = null;
            var fields = result.fields();
            while (fields.hasNext()) {
                var field = fields.next();
                if (!"last".equals(field.getKey())) {
                    rows = field.getValue();
                }
            }
            if (rows == null || rows.isEmpty()) {
                return;
            }
            long highest = -1;
            for (JsonNode row : rows) {
                long id = row.path(6).asLong();
                highest = Math.max(highest, id);
                if (id < fromId || id > toId) {
                    continue;
                }
                KrakenTradePayload payload = new KrakenTradePayload(
                        venueSymbol,
                        "b".equals(row.path(3).asText()) ? "buy" : "sell",
                        // REST pads ("82781.20000"); the socket sends bare numbers (82781.2).
                        new BigDecimal(row.path(0).asText()).stripTrailingZeros(),
                        new BigDecimal(row.path(1).asText()).stripTrailingZeros(),
                        "m".equals(row.path(4).asText()) ? "market" : "limit",
                        id,
                        toInstant(row.path(2).decimalValue()));
                into.add(mapper.fromKraken(payload, true, row.toString()));
            }
            String next = result.path("last").asText("");
            if (highest >= toId || next.isEmpty() || next.equals(since)) {
                return;
            }
            since = next;
        }
    }

    /** Kraken sends the time as fractional seconds, e.g. {@code 1791625771.463717}. */
    static Instant toInstant(BigDecimal epochSeconds) {
        long seconds = epochSeconds.longValue();
        int nanos = epochSeconds.subtract(BigDecimal.valueOf(seconds)).movePointRight(9).intValue();
        return Instant.ofEpochSecond(seconds, nanos);
    }
}
