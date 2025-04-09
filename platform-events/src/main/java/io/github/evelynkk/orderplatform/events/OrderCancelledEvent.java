package io.github.evelynkk.orderplatform.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record OrderCancelledEvent(
        @JsonProperty("orderId") String orderId,
        @JsonProperty("reason") String reason,
        @JsonProperty("occurredAt") Instant occurredAt
) {
}
