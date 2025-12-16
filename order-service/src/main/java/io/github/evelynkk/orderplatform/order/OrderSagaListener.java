package io.github.evelynkk.orderplatform.order;

import io.github.evelynkk.orderplatform.events.InventoryDeductedEvent;
import io.github.evelynkk.orderplatform.events.InventoryInsufficientEvent;
import io.github.evelynkk.orderplatform.events.PaymentCompletedEvent;
import io.github.evelynkk.orderplatform.events.PaymentFailedEvent;
import io.github.evelynkk.orderplatform.events.ShippingCreatedEvent;
import io.github.evelynkk.orderplatform.messaging.idempotency.IdempotentEventProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Projects the outcome of every downstream service back onto the order aggregate.
 *
 * <p>order-service is the only participant that sees the whole saga: the others each react to one
 * step. Every handler runs through {@link IdempotentEventProcessor}, so a redelivered event cannot
 * double-apply a transition.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderSagaListener {

    static final String GROUP = "order-service";

    private final OrderService orderService;
    private final IdempotentEventProcessor idempotency;

    @KafkaListener(topics = "inventory.deducted", groupId = GROUP)
    public void onInventoryDeducted(InventoryDeductedEvent event) {
        idempotency.processOnce(event.eventId(), GROUP,
                () -> orderService.advance(event.orderId(), OrderStatus.INVENTORY_RESERVED));
    }

    @KafkaListener(topics = "payment.completed", groupId = GROUP)
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        idempotency.processOnce(event.eventId(), GROUP,
                () -> orderService.advance(event.orderId(), OrderStatus.PAID));
    }

    @KafkaListener(topics = "shipping.created", groupId = GROUP)
    public void onShippingCreated(ShippingCreatedEvent event) {
        idempotency.processOnce(event.eventId(), GROUP,
                () -> orderService.advance(event.orderId(), OrderStatus.SHIPPED));
    }

    /**
     * Nothing was reserved, so cancelling is the whole of the compensation.
     */
    @KafkaListener(topics = "inventory.insufficient", groupId = GROUP)
    public void onInventoryInsufficient(InventoryInsufficientEvent event) {
        log.warn("Inventory insufficient for order {}: requested {}, available {}",
                event.orderId(), event.requestedQuantity(), event.availableStock());
        idempotency.processOnce(event.eventId(), GROUP,
                () -> orderService.cancel(event.orderId(), "INSUFFICIENT_INVENTORY"));
    }

    /**
     * Stock was already reserved by this point. Cancelling publishes {@code order.cancelled},
     * which is what tells inventory-service to release it.
     */
    @KafkaListener(topics = "payment.failed", groupId = GROUP)
    public void onPaymentFailed(PaymentFailedEvent event) {
        log.warn("Payment failed for order {}: {}", event.orderId(), event.reason());
        idempotency.processOnce(event.eventId(), GROUP,
                () -> orderService.cancel(event.orderId(), "PAYMENT_FAILED: " + event.reason()));
    }
}
