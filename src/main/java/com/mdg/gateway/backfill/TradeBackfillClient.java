package com.mdg.gateway.backfill;

import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;

import java.time.Instant;
import java.util.List;

/**
 * Fetches a venue's historical trades over REST, as the same canonical events the socket
 * path produces — same mapper, same deterministic {@code eventId} — but flagged
 * {@code backfilled}.
 */
interface TradeBackfillClient {

    Exchange exchange();

    /**
     * Adds every trade with an id in {@code [fromId, toId]} that the venue returns to
     * {@code into}, in no particular order.
     *
     * <p>Results are added as each page arrives, so if a later page fails the caller still
     * has the earlier ones.
     *
     * @param lastSeenAt venue timestamp of the trade just before {@code fromId}; needed by
     *                   venues that page by time rather than id, otherwise ignored
     * @throws BackfillException on any transport, HTTP or payload failure
     */
    void fetch(String venueSymbol, long fromId, long toId, Instant lastSeenAt, List<CanonicalTradeEvent> into);
}
