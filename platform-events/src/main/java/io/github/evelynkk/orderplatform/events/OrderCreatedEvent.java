package io.github.evelynkk.orderplatform.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreatedEvent(
        @JsonProperty("eventId") String eventId,
        @JsonProperty("orderId") String orderId,
        @JsonProperty("userId") String userId,
        @JsonProperty("productId") String productId,
        @JsonProperty("quantity") int quantity,
        @JsonProperty("totalAmount") BigDecimal totalAmount,
        @JsonProperty("occurredAt") Instant occurredAt
) implements DomainEvent {
}
