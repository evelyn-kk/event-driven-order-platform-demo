package io.github.evelynkk.orderplatform.order;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    /**
     * Deterministic failure injection for demos and tests.
     *
     * <p>Both values are chosen to exceed the limits the downstream services enforce — seeded
     * stock and the payment ceiling respectively — so the corresponding branch of the saga always
     * fires without either service needing to know a test is running.
     */
    private static final int QUANTITY_EXCEEDING_ANY_STOCK = 10_000;
    private static final BigDecimal AMOUNT_EXCEEDING_PAYMENT_LIMIT = new BigDecimal("99999.00");

    private final OrderService orderService;

    public enum Scenario {
        HAPPY, OUT_OF_STOCK, PAYMENT_FAILED
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public OrderView createOrder(@Valid @RequestBody CreateOrderRequest request,
                                 @RequestParam(name = "scenario", defaultValue = "HAPPY") Scenario scenario) {
        int quantity = scenario == Scenario.OUT_OF_STOCK
                ? QUANTITY_EXCEEDING_ANY_STOCK
                : request.quantity();
        BigDecimal amount = scenario == Scenario.PAYMENT_FAILED
                ? AMOUNT_EXCEEDING_PAYMENT_LIMIT
                : request.totalAmount();

        return OrderView.of(orderService.placeOrder(
                request.userId(), request.productId(), quantity, amount));
    }

    /** Lets a caller follow the saga: the order advances through its states asynchronously. */
    @GetMapping("/{orderId}")
    public ResponseEntity<OrderView> getOrder(@PathVariable String orderId) {
        return orderService.find(orderId)
                .map(OrderView::of)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
