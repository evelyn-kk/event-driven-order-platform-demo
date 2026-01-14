package io.github.evelynkk.orderplatform.order;

import java.math.BigDecimal;
import java.time.Instant;

/** Read model returned by the HTTP API, kept separate so the entity stays free to change. */
public record OrderView(
        String orderId,
        String userId,
        String productId,
        int quantity,
        BigDecimal totalAmount,
        OrderStatus status,
        String cancelReason,
        Instant createdAt,
        Instant updatedAt
) {
    public static OrderView of(Order order) {
        return new OrderView(
                order.getOrderId(),
                order.getUserId(),
                order.getProductId(),
                order.getQuantity(),
                order.getTotalAmount(),
                order.getStatus(),
                order.getCancelReason(),
                order.getCreatedAt(),
                order.getUpdatedAt());
    }
}
