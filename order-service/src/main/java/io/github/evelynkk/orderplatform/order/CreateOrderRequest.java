package io.github.evelynkk.orderplatform.order;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

/**
 * Constraints mirror the CHECK constraints on the orders table, so bad input is rejected with a
 * 400 at the edge instead of surfacing as a 500 from a constraint violation deep in the write.
 */
public record CreateOrderRequest(
        @NotBlank String userId,
        @NotBlank String productId,
        @Positive int quantity,
        @NotNull @DecimalMin("0.00") BigDecimal totalAmount
) {
}
