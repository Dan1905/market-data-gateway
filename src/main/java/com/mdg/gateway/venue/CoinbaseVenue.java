package com.mdg.gateway.venue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.dto.CoinbaseMarketTradesEnvelope;
import com.mdg.gateway.dto.CoinbaseTradePayload;
import com.mdg.gateway.backfill.BackfillHttp;
import com.mdg.gateway.backfill.CoinbaseBackfillClient;
import com.mdg.gateway.backfill.TradeBackfillClient;
import com.mdg.gateway.client.CoinbaseWebSocketClient;
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
import java.util.ArrayList;
import java.util.List;

/**
 * Coinbase Advanced Trade (socket) and Exchange (REST).
 *
 * <p>Trades arrive in an envelope of events, each a batch listed newest-first; the first
 * event after subscribing is a snapshot of recent trades. History is paged backwards from a
 * trade id, on a REST API that shares the socket's id space.
 */
@Component
public class CoinbaseVenue implements Venue {

    private static final Logger log = LoggerFactory.getLogger(CoinbaseVenue.class);

    private final ObjectMapper objectMapper;
    private final ExchangePayloadMapper payloadMapper;
    private final FrameReader frames;

    public CoinbaseVenue(ObjectMapper objectMapper, ExchangePayloadMapper payloadMapper, FrameReader frames) {
        this.objectMapper = objectMapper;
        this.payloadMapper = payloadMapper;
        this.frames = frames;
    }

    @Override
    public Exchange exchange() {
        return Exchange.COINBASE;
    }

    @Override
    public List<CanonicalTradeEvent> parse(String rawFrame) {
        JsonNode root = frames.tree(exchange(), rawFrame);
        CoinbaseMarketTradesEnvelope envelope =
                frames.read(exchange(), rawFrame, root, CoinbaseMarketTradesEnvelope.class);

        if (!envelope.isMarketTrades()) {
            log.debug("Skipping non-trade Coinbase frame (channel={})", envelope.channel());
            return List.of();
        }

        // Coinbase batches N trades per frame. One bad entry fails the whole frame: the frame
        // is the unit stored in the DLQ, so partial success would make a replay duplicate the
        // entries that already succeeded (deduplication would catch it, but only inside its
        // window).
        List<CanonicalTradeEvent> events = new ArrayList<>();
        List<CoinbaseMarketTradesEnvelope.Event> batch = envelope.safeEvents();
        for (int i = 0; i < batch.size(); i++) {
            CoinbaseMarketTradesEnvelope.Event event = batch.get(i);
            if (event == null || event.trades() == null) {
                continue;
            }
            // The snapshot sent on subscribe replays recent trades — possibly ones already
            // published before a reconnect. Flag them; the sequencer and dedup drop the overlap.
            boolean backfilled = event.isSnapshot();
            JsonNode tradeNodes = root.path("events").path(i).path("trades");
            for (int j = 0; j < event.trades().size(); j++) {
                CoinbaseTradePayload trade = event.trades().get(j);
                if (trade == null) {
                    continue;
                }
                // Each event keeps its own trade object, not the whole batch.
                String raw = frames.slice(tradeNodes.path(j));
                events.add(frames.map(exchange(), rawFrame,
                        () -> payloadMapper.fromCoinbase(trade, backfilled, raw)));
            }
        }
        if (events.isEmpty()) {
            log.debug("Coinbase market_trades frame carried no trades");
        }
        // Coinbase lists a batch newest-first. Published as-is, a consumer would see trade
        // 935 before 934 on the same symbol.
        return FrameReader.inTradeOrder(events);
    }

    @Override
    public ExchangeWebSocketClient createClient(ExchangeProperties.Connection config,
                                                CircuitBreaker connectionBreaker,
                                                FeedRuntime runtime) {
        return new CoinbaseWebSocketClient(config, runtime.ingestionService(), runtime.ingestionExecutor(),
                runtime.scheduler(), runtime.httpClient(), objectMapper, connectionBreaker, runtime.meterRegistry());
    }

    @Override
    public TradeBackfillClient createBackfillClient(URI restUrl, BackfillHttp http) {
        return new CoinbaseBackfillClient(restUrl, http, payloadMapper);
    }
}
