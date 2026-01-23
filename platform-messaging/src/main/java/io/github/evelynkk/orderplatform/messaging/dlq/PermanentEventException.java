package io.github.evelynkk.orderplatform.messaging.dlq;

/**
 * Marks a failure that retrying cannot fix.
 *
 * <p>The distinction matters more than it looks. Retry is only useful against a condition that
 * might change on its own — a lock timeout, a broker election, an event that has not arrived yet.
 * A record that fails deterministically will fail identically on every attempt, and because
 * retries here are blocking, retrying it stalls the whole partition behind a message that is never
 * going to succeed. Throwing this routes it straight to the dead-letter topic instead.
 */
public class PermanentEventException extends RuntimeException {

    public PermanentEventException(String message) {
        super(message);
    }

    public PermanentEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
