package com.mdg.gateway.exception;

/**
 * Raised when a replay is requested while another is in flight.
 *
 * <p>Concurrent replays would re-read the same DLQ offsets and duplicate every republished
 * event, so the service admits one at a time and rejects the rest with HTTP 409.
 */
public class ReplayAlreadyRunningException extends RuntimeException {

    public ReplayAlreadyRunningException(String message) {
        super(message);
    }
}
