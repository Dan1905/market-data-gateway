package com.mdg.gateway.support;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A local stand-in for an exchange's WebSocket endpoint.
 *
 * <p>Lets the ingestion clients be tested for real — handshake, subscription frames,
 * fragmented message reassembly, ordering, connection loss and reconnect — without dialling
 * Binance, Coinbase or Kraken. A test that depends on a public exchange fails on their rate
 * limits, their geo-blocking and their maintenance windows, none of which are defects in
 * this code.
 *
 * <p>This is the only place the project uses {@code spring-boot-starter-websocket}, and it
 * is test-scoped: the gateway itself never serves WebSockets, it only dials out.
 */
@TestConfiguration
@EnableWebSocket
public class TestVenueServer implements WebSocketConfigurer {

    public static final String PATH = "/venue";

    @Bean
    public VenueHandler venueHandler() {
        return new VenueHandler();
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(venueHandler(), PATH).setAllowedOrigins("*");
    }

    /** Records what the client sent and lets a test drive what the "venue" sends back. */
    public static class VenueHandler extends TextWebSocketHandler {

        /** Frames the client sent us — subscription requests, in practice. */
        private final BlockingQueue<String> received = new LinkedBlockingQueue<>();

        /** Every session this handler has accepted, so tests can count reconnects. */
        private final List<WebSocketSession> sessions = new CopyOnWriteArrayList<>();

        private final AtomicReference<WebSocketSession> current = new AtomicReference<>();
        private final AtomicInteger connectionCount = new AtomicInteger();

        /** When true, the server hangs up immediately — used to exercise reconnect. */
        private volatile boolean closeImmediately;

        @Override
        public void afterConnectionEstablished(WebSocketSession session) throws Exception {
            connectionCount.incrementAndGet();
            sessions.add(session);
            current.set(session);
            if (closeImmediately) {
                session.close(CloseStatus.SERVER_ERROR);
            }
        }

        @Override
        protected void handleTextMessage(WebSocketSession session, TextMessage message) {
            received.add(message.getPayload());
        }

        @Override
        public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
            current.compareAndSet(session, null);
        }

        // ---------------- driving the venue from a test ----------------

        /** Sends one complete frame to the connected client. */
        public void send(String payload) throws IOException {
            WebSocketSession session = requireSession();
            synchronized (session) {
                session.sendMessage(new TextMessage(payload));
            }
        }

        /**
         * Sends one logical frame split across several WebSocket continuation frames.
         *
         * <p>Real venues fragment large messages and the JDK client surfaces that as
         * multiple {@code onText} callbacks with {@code last=false}. Reassembling those is
         * the client's job, and it is worth testing deliberately rather than hoping a
         * payload happens to be big enough to trigger it.
         */
        public void sendFragmented(String payload, int chunks) throws IOException {
            WebSocketSession session = requireSession();
            int size = (int) Math.ceil(payload.length() / (double) chunks);
            synchronized (session) {
                for (int offset = 0; offset < payload.length(); offset += size) {
                    String chunk = payload.substring(offset, Math.min(offset + size, payload.length()));
                    boolean last = offset + size >= payload.length();
                    session.sendMessage(new TextMessage(chunk, last));
                }
            }
        }

        /** Hangs up on the client, simulating a venue-side disconnect. */
        public void disconnect() throws IOException {
            WebSocketSession session = current.getAndSet(null);
            if (session != null && session.isOpen()) {
                session.close(CloseStatus.SERVER_ERROR);
            }
        }

        public void setCloseImmediately(boolean closeImmediately) {
            this.closeImmediately = closeImmediately;
        }

        public boolean isConnected() {
            WebSocketSession session = current.get();
            return session != null && session.isOpen();
        }

        public int connectionCount() {
            return connectionCount.get();
        }

        public BlockingQueue<String> received() {
            return received;
        }

        /** Clears state between tests so counts start from zero. */
        public void reset() throws IOException {
            disconnect();
            received.clear();
            sessions.clear();
            connectionCount.set(0);
            closeImmediately = false;
        }

        private WebSocketSession requireSession() {
            WebSocketSession session = current.get();
            if (session == null || !session.isOpen()) {
                throw new IllegalStateException("No client is connected to the test venue");
            }
            return session;
        }
    }
}
