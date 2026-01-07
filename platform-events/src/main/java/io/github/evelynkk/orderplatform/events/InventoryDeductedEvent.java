package io.github.evelynkk.orderplatform.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record InventoryDeductedEvent(
        @JsonProperty("eventId") String eventId,
        @JsonProperty("orderId") String orderId,
        @JsonProperty("productId") String productId,
        @JsonProperty("quantity") int quantity,
        @JsonProperty("remainingStock") int remainingStock,
        @JsonProperty("occurredAt") Instant occurredAt
) implements DomainEvent {
}
