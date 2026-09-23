package com.mdg.gateway.client;

import com.mdg.gateway.model.Exchange;

/**
 * Lifecycle contract for a single venue feed.
 *
 * <p>Kept deliberately narrow so {@code ExchangeConnectionManager} can drive all venues
 * uniformly and so tests can substitute a fake feed without a socket.
 */
public interface ExchangeWebSocketClient extends AutoCloseable {

    Exchange exchange();

    /** Idempotent: connects if not already connected, and arms reconnection. */
    void start();

    /** Idempotent: disarms reconnection and closes the socket gracefully. */
    @Override
    void close();

    /** True when a socket is currently open and has not been closed by either side. */
    boolean isConnected();

    /** Consecutive failed connect attempts; resets to zero on a successful handshake. */
    int consecutiveFailures();
}
