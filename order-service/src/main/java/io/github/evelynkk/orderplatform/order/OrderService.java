package io.github.evelynkk.orderplatform.order;

import io.github.evelynkk.orderplatform.events.DomainEvent;
import io.github.evelynkk.orderplatform.events.OrderCancelledEvent;
import io.github.evelynkk.orderplatform.events.OrderCreatedEvent;
import io.github.evelynkk.orderplatform.messaging.dlq.PermanentEventException;
import io.github.evelynkk.orderplatform.messaging.outbox.OutboxRecorder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Owns the order aggregate and every transition applied to it.
 *
 * <p>Each method here is one transaction that writes the order's new state and, where the
 * transition is externally interesting, appends the resulting event to the outbox. Because both
 * writes share a transaction, an order can never be persisted in a state the rest of the platform
 * was never told about, nor can an event announce a state that failed to commit.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private static final String AGGREGATE_TYPE = "order";

    private final OrderRepository orders;
    private final OutboxRecorder outbox;

    @Transactional
    public Order placeOrder(String userId, String productId, int quantity, BigDecimal totalAmount) {
        Order order = new Order(UUID.randomUUID().toString(), userId, productId, quantity, totalAmount);
        orders.save(order);

        outbox.record(AGGREGATE_TYPE, "order.created", new OrderCreatedEvent(
                DomainEvent.newEventId(),
                order.getOrderId(),
                order.getUserId(),
                order.getProductId(),
                order.getQuantity(),
                order.getTotalAmount(),
                Instant.now()));

        log.info("Placed order {} for user {}", order.getOrderId(), userId);
        return order;
    }

    /**
     * Applies a forward transition reported by another service.
     *
     * <p>No event is emitted: the order's own progress is already described by the event that
     * caused it, and re-announcing it would only add a second thing for consumers to reconcile.
     */
    @Transactional
    public void advance(String orderId, OrderStatus next) {
        Order order = require(orderId);
        if (order.advanceTo(next)) {
            log.info("Order {} advanced to {}", orderId, next);
        } else {
            log.debug("Order {} ignored transition to {} (already {})", orderId, next, order.getStatus());
        }
    }

    /**
     * Cancels the order and announces it, which is what drives compensation in the services that
     * already committed work for this order.
     */
    @Transactional
    public void cancel(String orderId, String reason) {
        Order order = require(orderId);
        if (!order.cancel(reason)) {
            log.debug("Order {} already terminal ({}); ignoring cancellation", orderId, order.getStatus());
            return;
        }

        outbox.record(AGGREGATE_TYPE, "order.cancelled", new OrderCancelledEvent(
                DomainEvent.newEventId(),
                orderId,
                reason,
                Instant.now()));

        log.warn("Cancelled order {}: {}", orderId, reason);
    }

    @Transactional(readOnly = true)
    public Optional<Order> find(String orderId) {
        return orders.findById(orderId);
    }

    private Order require(String orderId) {
        // Permanent, not transient: this service writes the order in the same transaction that
        // announces it, so an event referencing an order it has never seen describes a state that
        // will never exist. Retrying would stall the partition; the DLQ is where it belongs.
        return orders.findById(orderId)
                .orElseThrow(() -> new PermanentEventException("Unknown order " + orderId));
    }
}
