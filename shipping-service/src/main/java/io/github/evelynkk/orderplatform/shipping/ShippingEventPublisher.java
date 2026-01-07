package io.github.evelynkk.orderplatform.shipping;

import io.github.evelynkk.orderplatform.events.DomainEvent;
import io.github.evelynkk.orderplatform.events.ShippingCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class ShippingEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishShippingCreated(String orderId) {
        ShippingCreatedEvent event = new ShippingCreatedEvent(
                DomainEvent.newEventId(),
                orderId,
                UUID.randomUUID().toString(),
                "SF-Express",
                "SF" + UUID.randomUUID().toString().substring(0, 10).toUpperCase(),
                Instant.now()
        );
        kafkaTemplate.send("shipping.created", orderId, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Published shipping.created for orderId={}", orderId);
                    } else {
                        log.error("Failed to publish shipping.created for orderId={}", orderId, ex);
                    }
                });
    }
}
