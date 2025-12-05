package io.github.evelynkk.orderplatform.inventory;

import io.github.evelynkk.orderplatform.events.OrderCreatedEvent;
import io.github.evelynkk.orderplatform.events.PaymentFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryEventListener {

    private final InventoryEventPublisher publisher;

    // Simulated inventory storage
    private final Map<String, Integer> stock = new ConcurrentHashMap<>();

    @KafkaListener(topics = "order.created", groupId = "inventory-service")
    public void onOrderCreated(OrderCreatedEvent event) {
        log.info("Received order.created for orderId={}, productId={}, quantity={}",
                event.orderId(), event.productId(), event.quantity());

        int available = stock.computeIfAbsent(event.productId(), k -> 100);

        if (event.quantity() > available) {
            log.warn("Insufficient stock for orderId={}, requested={}, available={}",
                    event.orderId(), event.quantity(), available);
            publisher.publishInventoryInsufficient(event.orderId(), event.productId(), event.quantity(), available);
        } else {
            int remaining = available - event.quantity();
            stock.put(event.productId(), remaining);
            log.info("Stock deducted for orderId={}, remaining={}", event.orderId(), remaining);
            publisher.publishInventoryDeducted(event.orderId(), event.productId(), event.quantity(), remaining);
        }
    }

    @KafkaListener(topics = "payment.failed", groupId = "inventory-service-compensation")
    public void onPaymentFailed(PaymentFailedEvent event) {
        log.warn("Received payment.failed for orderId={}. Compensating: releasing stock (simulated).", event.orderId());
        // In production: look up reserved quantity by orderId and restore stock.
        // Here we only log because the demo does not persist reservations.
    }
}
