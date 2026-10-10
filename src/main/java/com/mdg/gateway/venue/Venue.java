package com.mdg.gateway.venue;

import com.mdg.gateway.backfill.BackfillHttp;
import com.mdg.gateway.backfill.TradeBackfillClient;
import com.mdg.gateway.client.ExchangeWebSocketClient;
import com.mdg.gateway.config.ExchangeProperties;
import com.mdg.gateway.exception.PayloadParsingException;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;

import java.net.URI;
import java.util.List;

/**
 * Everything that is specific to one exchange, in one place.
 *
 * <p>An exchange differs from the others in exactly three ways: how its socket is opened and
 * subscribed, what its frames look like, and how its REST API pages historical trades. Each
 * implementation owns those three and nothing else. The rest of the gateway — connection
 * management, ordering, deduplication, publishing, gap detection — is written once against
 * this interface and never branches on which exchange it is dealing with.
 *
 * <p>Adding an exchange is therefore additive: a new implementation of this interface (a
 * Spring bean, picked up automatically), its payload DTO and mapper method, an
 * {@link Exchange} constant, and a {@code gateway.exchanges.<name>} block in configuration.
 * No existing class needs a new {@code case}.
 */
public interface Venue {

    Exchange exchange();

    /**
     * Turns one socket frame into canonical events.
     *
     * @return the trades in the frame, in ascending trade-id order per symbol; empty for a
     *         control frame (heartbeat, subscription ack, status), which is not a failure
     * @throws PayloadParsingException if the frame is malformed or a trade in it violates the
     *                                 canonical model's invariants
     */
    List<CanonicalTradeEvent> parse(String rawFrame);

    /** Builds this exchange's socket client. The caller owns its lifecycle. */
    ExchangeWebSocketClient createClient(ExchangeProperties.Connection config,
                                         CircuitBreaker connectionBreaker,
                                         FeedRuntime runtime);

    /** Builds the client that recovers missed trades from this exchange's REST API. */
    TradeBackfillClient createBackfillClient(URI restUrl, BackfillHttp http);
}
