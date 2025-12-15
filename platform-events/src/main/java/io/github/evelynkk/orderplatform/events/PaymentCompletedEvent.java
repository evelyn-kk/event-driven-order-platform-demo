package io.github.evelynkk.orderplatform.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentCompletedEvent(
        @JsonProperty("eventId") String eventId,
        @JsonProperty("orderId") String orderId,
        @JsonProperty("userId") String userId,
        @JsonProperty("transactionId") String transactionId,
        @JsonProperty("amount") BigDecimal amount,
        @JsonProperty("occurredAt") Instant occurredAt
) implements DomainEvent {
}
