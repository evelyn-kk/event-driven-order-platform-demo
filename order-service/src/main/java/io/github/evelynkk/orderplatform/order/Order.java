package io.github.evelynkk.orderplatform.order;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "orders")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Order {

    @Id
    @Column(name = "order_id", nullable = false, updatable = false, length = 64)
    private String orderId;

    @Column(name = "user_id", nullable = false, length = 64)
    private String userId;

    @Column(name = "product_id", nullable = false, length = 64)
    private String productId;

    @Column(name = "quantity", nullable = false)
    private int quantity;

    @Column(name = "total_amount", nullable = false, precision = 19, scale = 2)
    private BigDecimal totalAmount;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private OrderStatus status;

    @Column(name = "cancel_reason", columnDefinition = "text")
    private String cancelReason;

    /**
     * Optimistic lock. Per-order events land on one partition and so are handled serially by a
     * single consumer thread, but that only holds within one consumer group — a replay tool or a
     * second writer would otherwise interleave read-modify-write cycles unnoticed.
     */
    @Version
    @Column(name = "version", nullable = false)
    private long version;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public Order(String orderId, String userId, String productId, int quantity, BigDecimal totalAmount) {
        this.orderId = orderId;
        this.userId = userId;
        this.productId = productId;
        this.quantity = quantity;
        this.totalAmount = totalAmount;
        this.status = OrderStatus.CREATED;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    /**
     * Moves the order forward one stage.
     *
     * @return true if the status changed, false if the event was stale or arrived after the order
     *         already reached a terminal state
     * @throws OutOfOrderEventException if the event would skip a stage
     */
    public boolean advanceTo(OrderStatus next) {
        if (status == next) {
            return false;
        }
        if (status.isTerminal()) {
            // A cancelled order can still receive a late success event from a service that was
            // mid-flight when the cancellation was decided. The cancellation wins.
            return false;
        }
        if (next.stage() <= status.stage()) {
            return false;
        }
        if (next.stage() > status.stage() + 1) {
            throw new OutOfOrderEventException(orderId, status, next);
        }
        this.status = next;
        this.updatedAt = Instant.now();
        return true;
    }

    /** Cancellation is reachable from any live state, so it bypasses the stage ordering. */
    public boolean cancel(String reason) {
        if (status.isTerminal()) {
            return false;
        }
        this.status = OrderStatus.CANCELLED;
        this.cancelReason = reason;
        this.updatedAt = Instant.now();
        return true;
    }
}
