package io.github.evelynkk.orderplatform.messaging.idempotency;

import java.io.Serializable;

/** Composite primary key for {@link ProcessedEvent}. */
public record ProcessedEventId(String eventId, String consumerGroup) implements Serializable {

    // JPA requires a no-arg constructor on an @IdClass.
    public ProcessedEventId() {
        this(null, null);
    }
}
