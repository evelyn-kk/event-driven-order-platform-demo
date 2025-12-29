package io.github.evelynkk.orderplatform.order;

import java.math.BigDecimal;

public record CreateOrderRequest(
        String userId,
        String productId,
        int quantity,
        BigDecimal totalAmount
) {
}
