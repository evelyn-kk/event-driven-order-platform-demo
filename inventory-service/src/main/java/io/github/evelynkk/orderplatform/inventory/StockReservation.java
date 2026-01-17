package io.github.evelynkk.orderplatform.inventory;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * What this service actually did for one order.
 *
 * <p>The original design logged "compensating" without recording anything, which meant it had no
 * way to know how much to give back. Persisting the reservation is what makes the compensation
 * real: releasing reads the quantity from here rather than trusting whatever the failure event
 * happens to carry.
 */
@Entity
@Table(name = "stock_reservation")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class StockReservation {

    public enum State {
        /** Stock is held for this order and can still be given back. */
        RESERVED,
        /** The order failed; the hold was returned to available stock. */
        RELEASED,
        /** The order shipped; the hold was consumed. */
        COMMITTED
    }

    @Id
    @Column(name = "order_id", nullable = false, updatable = false, length = 64)
    private String orderId;

    @Column(name = "product_id", nullable = false, length = 64)
    private String productId;

    @Column(name = "quantity", nullable = false)
    private int quantity;

    @Enumerated(EnumType.STRING)
    @Column(name = "state", nullable = false, length = 16)
    private State state;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "settled_at")
    private Instant settledAt;

    public StockReservation(String orderId, String productId, int quantity) {
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.state = State.RESERVED;
        this.createdAt = Instant.now();
    }

    /**
     * @return false if this reservation was already settled, which makes releasing a no-op rather
     *         than a double refund
     */
    public boolean release() {
        return settle(State.RELEASED);
    }

    public boolean commit() {
        return settle(State.COMMITTED);
    }

    private boolean settle(State target) {
        if (state != State.RESERVED) {
            return false;
        }
        this.state = target;
        this.settledAt = Instant.now();
        return true;
    }
}
