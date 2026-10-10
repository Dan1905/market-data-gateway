package com.mdg.gateway.venue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.dto.BinanceTradePayload;
import com.mdg.gateway.backfill.BackfillHttp;
import com.mdg.gateway.backfill.BinanceBackfillClient;
import com.mdg.gateway.backfill.TradeBackfillClient;
import com.mdg.gateway.client.BinanceWebSocketClient;
import com.mdg.gateway.client.ExchangeWebSocketClient;
import com.mdg.gateway.config.ExchangeProperties;
import com.mdg.gateway.mapper.ExchangePayloadMapper;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;

/**
 * Binance spot.
 *
 * <p>One trade per frame, as a flat object with numerics sent as strings. Subscriptions are
 * acknowledged with {@code {"result":null,"id":1}}, which carries no event type and is skipped.
 * History is paged forwards from a trade id.
 */
@Component
public class BinanceVenue implements Venue {

    private static final Logger log = LoggerFactory.getLogger(BinanceVenue.class);

    private final ObjectMapper objectMapper;
    private final ExchangePayloadMapper payloadMapper;
    private final FrameReader frames;

    public BinanceVenue(ObjectMapper objectMapper, ExchangePayloadMapper payloadMapper, FrameReader frames) {
        this.objectMapper = objectMapper;
        this.payloadMapper = payloadMapper;
        this.frames = frames;
    }

    @Override
    public Exchange exchange() {
        return Exchange.BINANCE;
    }

    @Override
    public List<CanonicalTradeEvent> parse(String rawFrame) {
        BinanceTradePayload payload = frames.read(exchange(), rawFrame, BinanceTradePayload.class);

        if (!payload.isTrade()) {
            log.debug("Skipping non-trade Binance frame (eventType={})", payload.eventType());
            return List.of();
        }
        // Binance has no snapshot on subscribe: every trade frame is live.
        return List.of(frames.map(exchange(), rawFrame, () -> payloadMapper.fromBinance(payload, false, rawFrame)));
    }

    @Override
    public ExchangeWebSocketClient createClient(ExchangeProperties.Connection config,
                                                CircuitBreaker connectionBreaker,
                                                FeedRuntime runtime) {
        return new BinanceWebSocketClient(config, runtime.ingestionService(), runtime.ingestionExecutor(),
                runtime.scheduler(), runtime.httpClient(), objectMapper, connectionBreaker, runtime.meterRegistry());
    }

    @Override
    public TradeBackfillClient createBackfillClient(URI restUrl, BackfillHttp http) {
        return new BinanceBackfillClient(restUrl, http, payloadMapper);
    }
}
