package io.github.evelynkk.orderplatform.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Contract shared by every event on the platform.
 *
 * <p>{@code eventId} is the deduplication key. Kafka gives us at-least-once delivery, so a
 * consumer may legitimately see the same record more than once after a rebalance, a retry, or a
 * redelivery following a processing failure. Consumers record the {@code eventId} they have
 * already handled in the same transaction as their business write, which turns at-least-once
 * delivery into effectively-once processing.
 *
 * <p>{@code orderId} doubles as the partition key, so all events for one order are totally
 * ordered within a partition.
 */
public interface DomainEvent {

    String eventId();

    String orderId();

    Instant occurredAt();

    static String newEventId() {
        return UUID.randomUUID().toString();
    }
}
