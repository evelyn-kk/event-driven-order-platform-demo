package io.github.evelynkk.orderplatform.payment;

import io.github.evelynkk.orderplatform.events.InventoryDeductedEvent;
import io.github.evelynkk.orderplatform.events.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventListener {

    private final PaymentEventPublisher publisher;

    // Demo threshold: payments above 10,000 will fail.
    private static final BigDecimal PAYMENT_LIMIT = new BigDecimal("10000");

    @KafkaListener(topics = "order.created", groupId = "payment-service")
    public void onOrderCreated(OrderCreatedEvent event) {
        log.info("Received order.created for orderId={}, userId={}, amount={}",
                event.orderId(), event.userId(), event.totalAmount());

        if (event.totalAmount().compareTo(PAYMENT_LIMIT) > 0) {
            log.warn("Payment failed for orderId={}, amount={} exceeds limit={}",
                    event.orderId(), event.totalAmount(), PAYMENT_LIMIT);
            publisher.publishPaymentFailed(event.orderId(), event.userId(), event.totalAmount(), "AMOUNT_EXCEEDS_LIMIT");
        } else {
            log.info("Payment succeeded for orderId={}", event.orderId());
            publisher.publishPaymentCompleted(event.orderId(), event.userId(), event.totalAmount());
        }
    }

    @KafkaListener(topics = "inventory.deducted", groupId = "payment-service-audit")
    public void onInventoryDeducted(InventoryDeductedEvent event) {
        // Audit log only. In a stricter choreography, payment would be triggered by inventory.deducted.
        log.info("Audit: inventory.deducted received for orderId={}", event.orderId());
    }
}
