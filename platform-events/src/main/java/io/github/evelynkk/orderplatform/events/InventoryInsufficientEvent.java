package io.github.evelynkk.orderplatform.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record InventoryInsufficientEvent(
        @JsonProperty("orderId") String orderId,
        @JsonProperty("productId") String productId,
        @JsonProperty("requestedQuantity") int requestedQuantity,
        @JsonProperty("availableStock") int availableStock,
        @JsonProperty("occurredAt") Instant occurredAt
) {
}
