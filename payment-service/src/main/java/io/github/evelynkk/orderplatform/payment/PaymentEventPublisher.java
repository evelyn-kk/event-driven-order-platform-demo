package io.github.evelynkk.orderplatform.payment;

import io.github.evelynkk.orderplatform.events.PaymentCompletedEvent;
import io.github.evelynkk.orderplatform.events.PaymentFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishPaymentCompleted(String orderId, String userId, BigDecimal amount) {
        PaymentCompletedEvent event = new PaymentCompletedEvent(orderId, userId, UUID.randomUUID().toString(), amount, Instant.now());
        kafkaTemplate.send("payment.completed", orderId, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Published payment.completed for orderId={}", orderId);
                    } else {
                        log.error("Failed to publish payment.completed for orderId={}", orderId, ex);
                    }
                });
    }

    public void publishPaymentFailed(String orderId, String userId, BigDecimal amount, String reason) {
        PaymentFailedEvent event = new PaymentFailedEvent(orderId, userId, amount, reason, Instant.now());
        kafkaTemplate.send("payment.failed", orderId, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Published payment.failed for orderId={}", orderId);
                    } else {
                        log.error("Failed to publish payment.failed for orderId={}", orderId, ex);
                    }
                });
    }
}
