package io.github.evelynkk.orderplatform.payment;

import io.github.evelynkk.orderplatform.events.DomainEvent;
import io.github.evelynkk.orderplatform.events.OrderCreatedEvent;
import io.github.evelynkk.orderplatform.events.PaymentCompletedEvent;
import io.github.evelynkk.orderplatform.events.PaymentFailedEvent;
import io.github.evelynkk.orderplatform.messaging.outbox.OutboxRecorder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private static final String AGGREGATE_TYPE = "payment";

    /** Stand-in for a payment provider declining the charge, so the failure path is reachable. */
    private static final BigDecimal DECLINE_ABOVE = new BigDecimal("10000");

    private final PaymentRepository payments;
    private final OutboxRecorder outbox;

    /** Records what the order costs. No money moves until inventory confirms it can be fulfilled. */
    @Transactional
    public void register(OrderCreatedEvent event) {
        payments.save(new Payment(event.orderId(), event.userId(), event.totalAmount()));
        log.debug("Registered pending payment for order {}", event.orderId());
    }

    /**
     * Charges an order whose stock is now held.
     *
     * <p>Driven by {@code inventory.deducted} rather than {@code order.created}: charging in
     * parallel with the stock check would take money for orders that turn out to be unfulfillable,
     * and then need a refund for something that should never have been charged.
     */
    @Transactional
    public void charge(String orderId) {
        Payment payment = payments.findById(orderId).orElseThrow(() -> new PaymentNotRegisteredException(orderId));

        if (payment.getAmount().compareTo(DECLINE_ABOVE) > 0) {
            if (!payment.fail("AMOUNT_EXCEEDS_LIMIT")) {
                return;
            }
            log.warn("Declined payment for order {}: {} exceeds {}",
                    orderId, payment.getAmount(), DECLINE_ABOVE);

            outbox.record(AGGREGATE_TYPE, "payment.failed", new PaymentFailedEvent(
                    DomainEvent.newEventId(),
                    orderId,
                    payment.getUserId(),
                    payment.getAmount(),
                    payment.getFailureReason(),
                    Instant.now()));
            return;
        }

        if (!payment.succeed()) {
            return;
        }
        log.info("Charged {} for order {}", payment.getAmount(), orderId);

        outbox.record(AGGREGATE_TYPE, "payment.completed", new PaymentCompletedEvent(
                DomainEvent.newEventId(),
                orderId,
                payment.getUserId(),
                payment.getTransactionId(),
                payment.getAmount(),
                Instant.now()));
    }

    /**
     * Compensation. Only a successful charge needs reversing; a cancellation that followed a
     * declined payment has nothing to give back.
     */
    @Transactional
    public void refund(String orderId) {
        payments.findById(orderId)
                .filter(Payment::refund)
                .ifPresent(payment -> log.info("Refunded {} for cancelled order {}",
                        payment.getAmount(), orderId));
    }
}
