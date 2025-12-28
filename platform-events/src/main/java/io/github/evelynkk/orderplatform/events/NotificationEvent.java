package io.github.evelynkk.orderplatform.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public record NotificationEvent(
        @JsonProperty("recipientId") String recipientId,
        @JsonProperty("channel") String channel,
        @JsonProperty("subject") String subject,
        @JsonProperty("content") String content,
        @JsonProperty("occurredAt") Instant occurredAt
) {
}
