package io.github.evelynkk.orderplatform.analytics;

import io.github.evelynkk.orderplatform.events.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AnalyticsEventListener {

    @KafkaListener(topics = "order.created", groupId = "analytics-service")
    public void onOrderCreated(OrderCreatedEvent event) {
        persist("order.created", event.orderId(), event);
    }

    @KafkaListener(topics = "inventory.deducted", groupId = "analytics-service")
    public void onInventoryDeducted(InventoryDeductedEvent event) {
        persist("inventory.deducted", event.orderId(), event);
    }

    @KafkaListener(topics = "payment.completed", groupId = "analytics-service")
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        persist("payment.completed", event.orderId(), event);
    }

    @KafkaListener(topics = "shipping.created", groupId = "analytics-service")
    public void onShippingCreated(ShippingCreatedEvent event) {
        persist("shipping.created", event.orderId(), event);
    }

    @KafkaListener(topics = "order.cancelled", groupId = "analytics-service")
    public void onOrderCancelled(OrderCancelledEvent event) {
        persist("order.cancelled", event.orderId(), event);
    }

    private void persist(String eventType, String orderId, Object payload) {
        log.info("[ANALYTICS] Persisted eventType={}, orderId={}, payload={}", eventType, orderId, payload);
    }
}
