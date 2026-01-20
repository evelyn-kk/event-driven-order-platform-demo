package io.github.evelynkk.orderplatform.payment;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * This service's own record of what an order costs and whether it has been charged.
 *
 * <p>The amount is copied from {@code order.created} rather than read back from order-service.
 * Each service keeping the slice of state it needs is what lets the platform stay asynchronous —
 * a synchronous lookup at charge time would reintroduce exactly the coupling the event flow
 * removes, and would fail whenever order-service was down.
 */
@Entity
@Table(name = "payment")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Payment {

    public enum State {
        PENDING, SUCCEEDED, FAILED, REFUNDED
    }

    @Id
    @Column(name = "order_id", nullable = false, updatable = false, length = 64)
    private String orderId;

    @Column(name = "user_id", nullable = false, length = 64)
    private String userId;

    @Column(name = "amount", nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(name = "state", nullable = false, length = 16)
    private State state;

    @Column(name = "transaction_id", length = 64)
    private String transactionId;

    @Column(name = "failure_reason", columnDefinition = "text")
    private String failureReason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "settled_at")
    private Instant settledAt;

    public Payment(String orderId, String userId, BigDecimal amount) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
        this.state = State.PENDING;
        this.createdAt = Instant.now();
    }

    /** @return false if this payment was already settled, so a replay cannot double-charge */
    public boolean succeed() {
        if (state != State.PENDING) {
            return false;
        }
        this.state = State.SUCCEEDED;
        this.transactionId = UUID.randomUUID().toString();
        this.settledAt = Instant.now();
        return true;
    }

    public boolean fail(String reason) {
        if (state != State.PENDING) {
            return false;
        }
        this.state = State.FAILED;
        this.failureReason = reason;
        this.settledAt = Instant.now();
        return true;
    }

    /** Compensation for an order cancelled after it was already charged. */
    public boolean refund() {
        if (state != State.SUCCEEDED) {
            return false;
        }
        this.state = State.REFUNDED;
        this.settledAt = Instant.now();
        return true;
    }
}
