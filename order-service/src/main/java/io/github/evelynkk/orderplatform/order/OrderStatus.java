package io.github.evelynkk.orderplatform.order;

/**
 * States an order moves through, and how far along the fulfillment path each one sits.
 *
 * <p>{@code stage} exists because events arrive over separate topics and therefore carry no
 * cross-topic ordering guarantee. Comparing stages lets the aggregate tell a stale replay
 * ("payment succeeded" when we already shipped) apart from a genuine gap ("shipped" when payment
 * has not landed yet) — the first is ignorable, the second is worth retrying.
 *
 * <p>{@code CANCELLED} sits off the main line at stage -1: it is reachable from any live state and
 * is never "behind" or "ahead" of one.
 */
public enum OrderStatus {

    CREATED(0),
    INVENTORY_RESERVED(1),
    PAID(2),
    SHIPPED(3),
    CANCELLED(-1);

    private final int stage;

    OrderStatus(int stage) {
        this.stage = stage;
    }

    public int stage() {
        return stage;
    }

    /** No event can move an order out of a terminal state. */
    public boolean isTerminal() {
        return this == SHIPPED || this == CANCELLED;
    }
}
