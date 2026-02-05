package io.github.evelynkk.orderplatform.shipping;

import io.github.evelynkk.orderplatform.events.PaymentCompletedEvent;
import io.github.evelynkk.orderplatform.messaging.idempotency.IdempotentEventProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShippingEventListener {

    static final String GROUP = "shipping-service";

    private final ShippingService shipping;
    private final IdempotentEventProcessor idempotency;

    @KafkaListener(topics = "payment.completed", groupId = GROUP)
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        idempotency.processOnce(event.eventId(), GROUP,
                () -> shipping.createShipment(event.orderId()));
    }
}
