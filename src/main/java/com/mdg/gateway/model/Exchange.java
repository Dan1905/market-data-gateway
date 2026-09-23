package com.mdg.gateway.model;

import java.util.Locale;
import java.util.Optional;

/**
 * Venues this gateway knows how to normalize.
 *
 * <p>Deliberately an enum rather than a free-form string: it is the switch key for
 * {@code PayloadTransformationService}, the Kafka partition key component, and the
 * value carried in the {@code X-Source-Exchange} DLQ header. A typo in configuration
 * should fail at binding time, not silently create a fourth "exchange".
 */
public enum Exchange {

    BINANCE,
    COINBASE,
    KRAKEN;

    /**
     * Case-insensitive lookup that never throws — used when reading back an exchange
     * name from an untrusted source such as a DLQ record header.
     */
    public static Optional<Exchange> fromName(String name) {
        if (name == null || name.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Exchange.valueOf(name.trim().toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException ex) {
            return Optional.empty();
        }
    }
}
