package io.github.evelynkk.orderplatform.payment;

import io.github.evelynkk.orderplatform.events.InventoryDeductedEvent;
import io.github.evelynkk.orderplatform.events.OrderCancelledEvent;
import io.github.evelynkk.orderplatform.events.OrderCreatedEvent;
import io.github.evelynkk.orderplatform.messaging.idempotency.IdempotentEventProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventListener {

    static final String GROUP = "payment-service";

    private final PaymentService payments;
    private final IdempotentEventProcessor idempotency;

    @KafkaListener(topics = "order.created", groupId = GROUP)
    public void onOrderCreated(OrderCreatedEvent event) {
        idempotency.processOnce(event.eventId(), GROUP, () -> payments.register(event));
    }

    @KafkaListener(topics = "inventory.deducted", groupId = GROUP)
    public void onInventoryDeducted(InventoryDeductedEvent event) {
        idempotency.processOnce(event.eventId(), GROUP, () -> payments.charge(event.orderId()));
    }

    @KafkaListener(topics = "order.cancelled", groupId = GROUP)
    public void onOrderCancelled(OrderCancelledEvent event) {
        idempotency.processOnce(event.eventId(), GROUP, () -> payments.refund(event.orderId()));
    }
}
