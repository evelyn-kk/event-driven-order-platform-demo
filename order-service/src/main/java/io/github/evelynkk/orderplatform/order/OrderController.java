package io.github.evelynkk.orderplatform.order;

import io.github.evelynkk.orderplatform.events.DomainEvent;
import io.github.evelynkk.orderplatform.events.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderEventPublisher publisher;

    public enum Scenario {
        HAPPY, OUT_OF_STOCK, PAYMENT_FAILED
    }

    @PostMapping
    public OrderCreatedEvent createOrder(@RequestBody CreateOrderRequest request,
                                         @RequestParam(name = "scenario", defaultValue = "HAPPY") Scenario scenario) {
        String orderId = UUID.randomUUID().toString();

        int quantity = request.quantity();
        BigDecimal amount = request.totalAmount();

        if (scenario == Scenario.OUT_OF_STOCK) {
            quantity = 999;
        } else if (scenario == Scenario.PAYMENT_FAILED) {
            amount = new BigDecimal("99999");
        }

        OrderCreatedEvent event = new OrderCreatedEvent(
                DomainEvent.newEventId(),
                orderId,
                request.userId(),
                request.productId(),
                quantity,
                amount,
                Instant.now()
        );

        log.info("Creating order: orderId={}, scenario={}", orderId, scenario);
        publisher.publishOrderCreated(event);
        return event;
    }
}
