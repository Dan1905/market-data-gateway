package com.mdg.gateway.mapper;

import com.mdg.gateway.config.GatewayProperties;
import org.mapstruct.Named;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Collapses three incompatible instrument notations into one canonical {@code BASE-QUOTE} form.
 *
 * <ul>
 *   <li>Binance ships concatenated symbols with no delimiter: {@code BTCUSDT}</li>
 *   <li>Coinbase ships hyphenated: {@code BTC-USD}</li>
 *   <li>Kraken v2 ships slashed: {@code BTC/USD}</li>
 * </ul>
 *
 * <p>Splitting Binance's form requires a longest-suffix match against known quote assets —
 * there is no delimiter to key on, and a naive 3-character split turns {@code BTCUSDT} into
 * {@code BTCU-SDT}. The quote list is ordered longest-first for that reason.
 *
 * <p><b>Stablecoin collapsing.</b> With {@code collapse-stablecoins} enabled (the default,
 * matching the {@code BTC-USD} target notation), {@code BTCUSDT} normalizes to
 * {@code BTC-USD}. Be aware that this conflates two genuinely different assets: USDT is an
 * issuer-credit token that has historically depegged from USD. If your downstream consumers
 * care about basis or counterparty risk, set
 * {@code gateway.symbol.collapse-stablecoins: false} and you will get {@code BTC-USDT}.
 */
@Component
public class SymbolNormalizer {

    /**
     * Known quote assets, longest first so that {@code USDT} wins over {@code USD} when
     * scanning the suffix of {@code BTCUSDT}.
     */
    private static final List<String> QUOTE_ASSETS = List.of(
            "FDUSD", "TUSD", "BUSD", "USDC", "USDT", "DAI",
            "USD", "EUR", "GBP", "JPY", "TRY", "BRL", "AUD",
            "BTC", "ETH", "BNB", "SOL", "XBT");

    /** Kraken still uses the ISO 4217-style {@code XBT} for bitcoin on some pairs. */
    private static final Map<String, String> ASSET_ALIASES = Map.of("XBT", "BTC");

    private static final Map<String, String> STABLECOIN_TO_FIAT = Map.of(
            "USDT", "USD",
            "USDC", "USD",
            "BUSD", "USD",
            "FDUSD", "USD",
            "TUSD", "USD",
            "DAI", "USD");

    private final GatewayProperties properties;

    public SymbolNormalizer(GatewayProperties properties) {
        this.properties = properties;
    }

    /**
     * Normalizes a delimiter-free venue symbol such as Binance's {@code BTCUSDT}.
     *
     * @return canonical {@code BASE-QUOTE}, or {@code null} when the input is null/blank so
     *         that the canonical model's invariant — not this method — reports the failure
     */
    @Named("normalizeConcatenated")
    public String normalizeConcatenated(String rawSymbol) {
        if (isBlank(rawSymbol)) {
            return null;
        }
        String upper = rawSymbol.trim().toUpperCase(Locale.ROOT);

        for (String quote : QUOTE_ASSETS) {
            // A symbol that *is* its own quote asset has no base; require a non-empty remainder.
            if (upper.length() > quote.length() && upper.endsWith(quote)) {
                String base = upper.substring(0, upper.length() - quote.length());
                return join(base, quote);
            }
        }
        // Unknown quote asset: emit it unsplit rather than guessing. Still a valid symbol,
        // and it surfaces in metrics as an instrument we should teach the normalizer about.
        return upper;
    }

    /**
     * Normalizes an already-delimited venue symbol — Coinbase's {@code BTC-USD} or
     * Kraken's {@code BTC/USD}.
     */
    @Named("normalizeDelimited")
    public String normalizeDelimited(String rawSymbol) {
        if (isBlank(rawSymbol)) {
            return null;
        }
        String upper = rawSymbol.trim().toUpperCase(Locale.ROOT);
        String[] parts = upper.split("[-/_]", 2);
        if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
            // Malformed or delimiter-free (e.g. "BTC/" or "BTCUSDT"). Strip any stray
            // delimiters so we never emit a symbol with a dangling separator, then try the
            // concatenated parser.
            return normalizeConcatenated(upper.replaceAll("[-/_]", ""));
        }
        return join(parts[0], parts[1]);
    }

    private String join(String base, String quote) {
        String canonicalBase = ASSET_ALIASES.getOrDefault(base, base);
        String canonicalQuote = ASSET_ALIASES.getOrDefault(quote, quote);

        if (properties.symbol().collapseStablecoins()) {
            canonicalQuote = STABLECOIN_TO_FIAT.getOrDefault(canonicalQuote, canonicalQuote);
        }
        return canonicalBase + "-" + canonicalQuote;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
