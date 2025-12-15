package io.github.evelynkk.orderplatform.order;

import io.github.evelynkk.orderplatform.events.DomainEvent;
import io.github.evelynkk.orderplatform.events.InventoryInsufficientEvent;
import io.github.evelynkk.orderplatform.events.OrderCancelledEvent;
import io.github.evelynkk.orderplatform.events.PaymentFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCancellationListener {

    private final OrderEventPublisher publisher;

    @KafkaListener(topics = "inventory.insufficient", groupId = "order-service-compensation")
    public void onInventoryInsufficient(InventoryInsufficientEvent event) {
        log.warn("Received inventory.insufficient for orderId={}, reason=stock not enough", event.orderId());
        publisher.publishOrderCancelled(new OrderCancelledEvent(
                DomainEvent.newEventId(),
                event.orderId(),
                "INSUFFICIENT_INVENTORY",
                Instant.now()
        ));
    }

    @KafkaListener(topics = "payment.failed", groupId = "order-service-compensation")
    public void onPaymentFailed(PaymentFailedEvent event) {
        log.warn("Received payment.failed for orderId={}, reason={}", event.orderId(), event.reason());
        publisher.publishOrderCancelled(new OrderCancelledEvent(
                DomainEvent.newEventId(),
                event.orderId(),
                "PAYMENT_FAILED: " + event.reason(),
                Instant.now()
        ));
    }
}
