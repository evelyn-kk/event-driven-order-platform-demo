package io.github.evelynkk.orderplatform.inventory;

import io.github.evelynkk.orderplatform.events.OrderCancelledEvent;
import io.github.evelynkk.orderplatform.events.OrderCreatedEvent;
import io.github.evelynkk.orderplatform.events.ShippingCreatedEvent;
import io.github.evelynkk.orderplatform.messaging.idempotency.IdempotentEventProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Deduplication matters most here. Reserving stock is not naturally idempotent — applying the same
 * {@code order.created} twice would hold twice the stock and leak the difference forever, since
 * only one release ever arrives.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryEventListener {

    static final String GROUP = "inventory-service";

    private final InventoryService inventory;
    private final IdempotentEventProcessor idempotency;

    @KafkaListener(topics = "order.created", groupId = GROUP)
    public void onOrderCreated(OrderCreatedEvent event) {
        idempotency.processOnce(event.eventId(), GROUP, () -> inventory.reserve(event));
    }

    /**
     * Compensation. Driven by {@code order.cancelled} rather than {@code payment.failed} so that
     * every cancellation path releases stock through one route — payment failure is only one of
     * the ways an order can end.
     */
    @KafkaListener(topics = "order.cancelled", groupId = GROUP)
    public void onOrderCancelled(OrderCancelledEvent event) {
        idempotency.processOnce(event.eventId(), GROUP, () -> inventory.release(event.orderId()));
    }

    @KafkaListener(topics = "shipping.created", groupId = GROUP)
    public void onShippingCreated(ShippingCreatedEvent event) {
        idempotency.processOnce(event.eventId(), GROUP, () -> inventory.commit(event.orderId()));
    }
}
