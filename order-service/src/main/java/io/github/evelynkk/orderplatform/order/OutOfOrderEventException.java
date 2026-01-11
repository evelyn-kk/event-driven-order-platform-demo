package io.github.evelynkk.orderplatform.order;

/**
 * Raised when an event would skip a stage — for example {@code shipping.created} arriving before
 * the {@code payment.completed} that must precede it.
 *
 * <p>Each topic orders its own partitions, but nothing orders events <em>across</em> topics, so a
 * later-stage event genuinely can overtake an earlier one. Treating that as a failure is
 * deliberate: the retry handler backs off and tries again, by which time the missing event has
 * usually landed. Applying the jump instead would silently erase a stage from the order's history.
 */
public class OutOfOrderEventException extends RuntimeException {

    public OutOfOrderEventException(String orderId, OrderStatus current, OrderStatus attempted) {
        super("Order %s is %s; cannot jump to %s without the intervening transition"
                .formatted(orderId, current, attempted));
    }
}
