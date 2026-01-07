package io.github.evelynkk.orderplatform.inventory;

import io.github.evelynkk.orderplatform.events.DomainEvent;
import io.github.evelynkk.orderplatform.events.InventoryDeductedEvent;
import io.github.evelynkk.orderplatform.events.InventoryInsufficientEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishInventoryDeducted(String orderId, String productId, int quantity, int remainingStock) {
        InventoryDeductedEvent event = new InventoryDeductedEvent(
                DomainEvent.newEventId(), orderId, productId, quantity, remainingStock, Instant.now());
        kafkaTemplate.send("inventory.deducted", orderId, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Published inventory.deducted for orderId={}", orderId);
                    } else {
                        log.error("Failed to publish inventory.deducted for orderId={}", orderId, ex);
                    }
                });
    }

    public void publishInventoryInsufficient(String orderId, String productId, int requestedQuantity, int availableStock) {
        InventoryInsufficientEvent event = new InventoryInsufficientEvent(
                DomainEvent.newEventId(), orderId, productId, requestedQuantity, availableStock, Instant.now());
        kafkaTemplate.send("inventory.insufficient", orderId, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Published inventory.insufficient for orderId={}", orderId);
                    } else {
                        log.error("Failed to publish inventory.insufficient for orderId={}", orderId, ex);
                    }
                });
    }
}
