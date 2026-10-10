package com.mdg.gateway.venue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.dto.KrakenMessageEnvelope;
import com.mdg.gateway.dto.KrakenTradePayload;
import com.mdg.gateway.backfill.BackfillHttp;
import com.mdg.gateway.backfill.KrakenBackfillClient;
import com.mdg.gateway.backfill.TradeBackfillClient;
import com.mdg.gateway.client.KrakenWebSocketClient;
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
 * Kraken v2.
 *
 * <p>Trades arrive in a {@code data} array with numerics as JSON numbers, on a socket that
 * also carries a heartbeat about once a second. A rejected subscription comes back as an
 * error frame, which is an operational problem to log, not a payload to dead-letter. History
 * is paged forwards by time.
 */
@Component
public class KrakenVenue implements Venue {

    private static final Logger log = LoggerFactory.getLogger(KrakenVenue.class);

    private final ObjectMapper objectMapper;
    private final ExchangePayloadMapper payloadMapper;
    private final FrameReader frames;

    public KrakenVenue(ObjectMapper objectMapper, ExchangePayloadMapper payloadMapper, FrameReader frames) {
        this.objectMapper = objectMapper;
        this.payloadMapper = payloadMapper;
        this.frames = frames;
    }

    @Override
    public Exchange exchange() {
        return Exchange.KRAKEN;
    }

    @Override
    public List<CanonicalTradeEvent> parse(String rawFrame) {
        JsonNode root = frames.tree(exchange(), rawFrame);
        KrakenMessageEnvelope envelope = frames.read(exchange(), rawFrame, root, KrakenMessageEnvelope.class);

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
        List<KrakenTradePayload> batch = envelope.safeData();
        for (int i = 0; i < batch.size(); i++) {
            KrakenTradePayload trade = batch.get(i);
            if (trade == null) {
                continue;
            }
            // Each event keeps its own trade object, not the whole batch.
            String raw = frames.slice(root.path("data").path(i));
            events.add(frames.map(exchange(), rawFrame,
                    () -> payloadMapper.fromKraken(trade, backfilled, raw)));
        }
        return FrameReader.inTradeOrder(events);
    }

    @Override
    public ExchangeWebSocketClient createClient(ExchangeProperties.Connection config,
                                                CircuitBreaker connectionBreaker,
                                                FeedRuntime runtime) {
        return new KrakenWebSocketClient(config, runtime.ingestionService(), runtime.ingestionExecutor(),
                runtime.scheduler(), runtime.httpClient(), objectMapper, connectionBreaker, runtime.meterRegistry());
    }

    @Override
    public TradeBackfillClient createBackfillClient(URI restUrl, BackfillHttp http) {
        return new KrakenBackfillClient(restUrl, http, payloadMapper);
    }
}
