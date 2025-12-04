package io.github.evelynkk.orderplatform.order;

import io.github.evelynkk.orderplatform.events.OrderCancelledEvent;
import io.github.evelynkk.orderplatform.events.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishOrderCreated(OrderCreatedEvent event) {
        String key = event.orderId();
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send("order.created", key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Published order.created for orderId={} to partition={}, offset={}",
                        event.orderId(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish order.created for orderId={}", event.orderId(), ex);
            }
        });
    }

    public void publishOrderCancelled(OrderCancelledEvent event) {
        String key = event.orderId();
        kafkaTemplate.send("order.cancelled", key, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Published order.cancelled for orderId={}", event.orderId());
                    } else {
                        log.error("Failed to publish order.cancelled for orderId={}", event.orderId(), ex);
                    }
                });
    }
}
