package io.github.evelynkk.orderplatform.payment;

/**
 * Raised when {@code inventory.deducted} arrives before the {@code order.created} that carries the
 * amount to charge.
 *
 * <p>Inventory only publishes after it has seen the order, but this service reads the two events
 * from different topics with no ordering between them, so the later one genuinely can arrive
 * first. Retrying with backoff resolves it; guessing an amount would not.
 */
public class PaymentNotRegisteredException extends RuntimeException {

    public PaymentNotRegisteredException(String orderId) {
        super("No registered payment for order " + orderId + " yet; order.created has not been processed");
    }
}
