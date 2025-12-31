package io.github.evelynkk.orderplatform.shipping;

import io.github.evelynkk.orderplatform.events.PaymentCompletedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShippingEventListener {

    private final ShippingEventPublisher publisher;

    @KafkaListener(topics = "payment.completed", groupId = "shipping-service")
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        log.info("Received payment.completed for orderId={}, creating shipment", event.orderId());
        // Simulate shipment creation
        publisher.publishShippingCreated(event.orderId());
    }
}
