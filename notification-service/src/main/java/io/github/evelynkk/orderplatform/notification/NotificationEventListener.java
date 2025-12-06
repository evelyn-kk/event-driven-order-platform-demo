package io.github.evelynkk.orderplatform.notification;

import io.github.evelynkk.orderplatform.events.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class NotificationEventListener {

    @KafkaListener(topics = "order.created", groupId = "notification-service")
    public void onOrderCreated(OrderCreatedEvent event) {
        send(event.userId(), "Order Confirmation",
                "Your order " + event.orderId() + " has been received.");
    }

    @KafkaListener(topics = "inventory.insufficient", groupId = "notification-service")
    public void onInventoryInsufficient(InventoryInsufficientEvent event) {
        send(event.orderId(), "Out of Stock",
                "Sorry, product " + event.productId() + " is out of stock for order " + event.orderId());
    }

    @KafkaListener(topics = "payment.completed", groupId = "notification-service")
    public void onPaymentCompleted(PaymentCompletedEvent event) {
        send(event.userId(), "Payment Successful",
                "Payment for order " + event.orderId() + " completed.");
    }

    @KafkaListener(topics = "payment.failed", groupId = "notification-service")
    public void onPaymentFailed(PaymentFailedEvent event) {
        send(event.userId(), "Payment Failed",
                "Payment for order " + event.orderId() + " failed: " + event.reason());
    }

    @KafkaListener(topics = "shipping.created", groupId = "notification-service")
    public void onShippingCreated(ShippingCreatedEvent event) {
        send(event.orderId(), "Shipped",
                "Order " + event.orderId() + " shipped via " + event.carrier() + ", tracking " + event.trackingNumber());
    }

    @KafkaListener(topics = "order.cancelled", groupId = "notification-service")
    public void onOrderCancelled(OrderCancelledEvent event) {
        send(event.orderId(), "Order Cancelled",
                "Order " + event.orderId() + " was cancelled: " + event.reason());
    }

    private void send(String recipientId, String subject, String content) {
        log.info("[NOTIFICATION] recipientId={}, subject={}, content={}", recipientId, subject, content);
    }
}
