package com.mdg.gateway.backfill;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdg.gateway.config.GatewayProperties;
import com.mdg.gateway.mapper.ExchangePayloadMapper;
import com.mdg.gateway.model.CanonicalTradeEvent;
import com.mdg.gateway.model.Exchange;
import com.mdg.gateway.service.TradeSequencer;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.net.http.HttpClient;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Recovers the trades inside a gap from the venue's REST API.
 *
 * <p>A socket reconnect, a redeploy or a crash all mean trades happened that this gateway
 * never saw. {@link TradeSequencer} notices (the next trade id is not the next id) and this
 * service fetches what is missing, as ordinary canonical events flagged {@code backfilled}.
 * The caller publishes them before the trade that revealed the gap, so the topic stays in
 * order and consumers cannot tell there was ever a hole — except by that flag.
 *
 * <h2>It must not make things worse</h2>
 * <ul>
 *   <li><b>Bounded.</b> At most {@code maxGap} trades per gap, the newest ones. A host that was
 *       off for a day does not replay a day through REST.</li>
 *   <li><b>Rate limited.</b> A Resilience4j limiter per venue, far under the public limits.</li>
 *   <li><b>Backs off.</b> HTTP 429/418 puts that venue's backfill on a cooldown
 *       ({@code Retry-After} if sent). Gaps found meanwhile are counted, not retried in a loop.</li>
 *   <li><b>Never blocks the feed on failure.</b> Any error returns what was fetched so far;
 *       the live trade is delivered regardless.</li>
 * </ul>
 * What could not be recovered is counted in {@code mdg.backfill.trades.missing} — the one
 * number that says whether the "no data loss" claim held.
 */
@Service
public class GapBackfillService {

    private static final Logger log = LoggerFactory.getLogger(GapBackfillService.class);

    private static final Duration DEFAULT_COOLDOWN = Duration.ofMinutes(2);
    private static final Duration MAX_COOLDOWN = Duration.ofMinutes(15);

    private final int maxGap;
    private final Map<Exchange, TradeBackfillClient> clients = new EnumMap<>(Exchange.class);
    private final Map<Exchange, Instant> cooldownUntil = new ConcurrentHashMap<>();
    private final MeterRegistry meterRegistry;

    public GapBackfillService(GatewayProperties properties,
                              HttpClient marketDataHttpClient,
                              ObjectMapper objectMapper,
                              ExchangePayloadMapper payloadMapper,
                              MeterRegistry meterRegistry) {
        GatewayProperties.Backfill config = properties.backfill();
        this.maxGap = config.maxGap();
        this.meterRegistry = meterRegistry;

        register(new BinanceBackfillClient(config.binanceUrl(),
                http(Exchange.BINANCE, config, marketDataHttpClient, objectMapper), payloadMapper));
        register(new CoinbaseBackfillClient(config.coinbaseUrl(),
                http(Exchange.COINBASE, config, marketDataHttpClient, objectMapper), payloadMapper));
        register(new KrakenBackfillClient(config.krakenUrl(),
                http(Exchange.KRAKEN, config, marketDataHttpClient, objectMapper), payloadMapper));
    }

    private void register(TradeBackfillClient client) {
        clients.put(client.exchange(), client);
    }

    private static BackfillHttp http(Exchange exchange, GatewayProperties.Backfill config,
                                     HttpClient httpClient, ObjectMapper objectMapper) {
        RateLimiter limiter = RateLimiter.of("backfill-" + exchange.name().toLowerCase(),
                RateLimiterConfig.custom()
                        .limitForPeriod(config.callsPerSecond())
                        .limitRefreshPeriod(Duration.ofSeconds(1))
                        .timeoutDuration(Duration.ofSeconds(30))
                        .build());
        return new BackfillHttp(httpClient, objectMapper, config.requestTimeout(), limiter);
    }

    /**
     * Fetches the trades missing before {@code next}.
     *
     * @return the recovered events in ascending trade-id order; possibly fewer than the gap,
     *         possibly none. Never throws.
     */
    public List<CanonicalTradeEvent> recover(CanonicalTradeEvent next, TradeSequencer.Check gap) {
        Exchange exchange = Exchange.fromName(next.exchange()).orElse(null);
        TradeBackfillClient client = exchange == null ? null : clients.get(exchange);
        long missing = gap.missing();
        counter("mdg.backfill.gaps", next).increment();
        if (client == null) {
            counter("mdg.backfill.trades.missing", next).increment(missing);
            return List.of();
        }

        long from = gap.firstMissing();
        long to = gap.lastMissing();
        if (missing > maxGap) {
            // Keep the newest part: it joins up with the live stream, so what remains is one
            // clean hole at the old end rather than a hole in the middle.
            from = to - maxGap + 1;
            log.warn("{} {} gap of {} trades exceeds the limit of {}; recovering only {}..{}",
                    next.exchange(), next.venueSymbol(), missing, maxGap, from, to);
        }

        Instant until = cooldownUntil.get(exchange);
        if (until != null && Instant.now().isBefore(until)) {
            log.warn("{} {} gap {}..{} not recovered: backfill is cooling down after a rate limit",
                    next.exchange(), next.venueSymbol(), gap.firstMissing(), to);
            counter("mdg.backfill.trades.missing", next).increment(missing);
            return List.of();
        }

        List<CanonicalTradeEvent> fetched = new ArrayList<>();
        long startedAt = System.nanoTime();
        try {
            client.fetch(next.venueSymbol(), from, to, gap.lastSeenAt(), fetched);
        } catch (BackfillException ex) {
            if (ex.isRateLimited()) {
                Duration cooldown = ex.retryAfter() == null ? DEFAULT_COOLDOWN : ex.retryAfter();
                if (cooldown.compareTo(MAX_COOLDOWN) > 0) {
                    cooldown = MAX_COOLDOWN;
                }
                cooldownUntil.put(exchange, Instant.now().plus(cooldown));
                counter("mdg.backfill.rate.limited", next).increment();
                log.warn("{} backfill rate limited; pausing it for {} s", exchange, cooldown.toSeconds());
            } else {
                log.warn("{} {} backfill of {}..{} failed part-way: {}",
                        next.exchange(), next.venueSymbol(), from, to, ex.getMessage());
            }
        } catch (RuntimeException ex) {
            // A malformed row, a mapping invariant: recovery is best-effort, the feed is not.
            log.error("{} {} backfill of {}..{} failed unexpectedly", next.exchange(), next.venueSymbol(), from, to, ex);
        }

        // One event per id, ascending: pages can overlap, and Coinbase pages newest-first.
        Map<Long, CanonicalTradeEvent> byId = new TreeMap<>(Comparator.naturalOrder());
        for (CanonicalTradeEvent event : fetched) {
            byId.putIfAbsent(Long.parseLong(event.tradeId()), event);
        }
        List<CanonicalTradeEvent> recovered = List.copyOf(byId.values());

        counter("mdg.backfill.trades.recovered", next).increment(recovered.size());
        counter("mdg.backfill.trades.missing", next).increment(missing - recovered.size());
        log.info("{} {} gap {}..{} ({} trades): recovered {} in {} ms",
                next.exchange(), next.venueSymbol(), gap.firstMissing(), to, missing, recovered.size(),
                (System.nanoTime() - startedAt) / 1_000_000);
        return recovered;
    }

    private Counter counter(String name, CanonicalTradeEvent event) {
        return meterRegistry.counter(name, "exchange", event.exchange());
    }
}
