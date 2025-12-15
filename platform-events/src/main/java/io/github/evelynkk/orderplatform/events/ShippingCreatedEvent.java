package io.github.evelynkk.orderplatform.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record ShippingCreatedEvent(
        @JsonProperty("eventId") String eventId,
        @JsonProperty("orderId") String orderId,
        @JsonProperty("shippingId") String shippingId,
        @JsonProperty("carrier") String carrier,
        @JsonProperty("trackingNumber") String trackingNumber,
        @JsonProperty("occurredAt") Instant occurredAt
) implements DomainEvent {
}
