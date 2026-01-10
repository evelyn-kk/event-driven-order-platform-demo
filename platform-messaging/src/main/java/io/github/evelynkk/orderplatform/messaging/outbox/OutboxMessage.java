package io.github.evelynkk.orderplatform.messaging.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * A message that has been committed to the database but not yet published to Kafka.
 *
 * <p>Writing business state and sending a Kafka record are two separate systems, so doing both
 * inline leaves a window where one succeeds and the other does not. Instead the business
 * transaction writes its state and appends a row here atomically; {@link OutboxRelay} publishes
 * the row afterwards. A crash between the two leaves an unpublished row, which the relay picks up
 * on its next pass.
 *
 * <p>The relay may publish a row and then fail before marking it, so the same record can reach
 * Kafka twice. That is the deliberate trade: the outbox guarantees at-least-once publication, and
 * consumer-side deduplication keyed on {@code eventId} makes reprocessing harmless.
 */
@Entity
@Table(name = "outbox_message")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OutboxMessage {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    /** Domain object the message belongs to, e.g. {@code order}. Aids triage, not routing. */
    @Column(name = "aggregate_type", nullable = false, length = 64)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false, length = 64)
    private String aggregateId;

    /** Deduplication key carried by the event itself; unique, so a retry cannot double-insert. */
    @Column(name = "event_id", nullable = false, unique = true, length = 64)
    private String eventId;

    @Column(name = "topic", nullable = false, length = 128)
    private String topic;

    @Column(name = "message_key", nullable = false, length = 128)
    private String messageKey;

    /**
     * Fully qualified event class, republished as the {@code __TypeId__} header so consumers
     * deserialize into the right type without the relay having to understand the payload.
     */
    @Column(name = "payload_type", nullable = false, length = 255)
    private String payloadType;

    /** Serialized event, stored exactly as it will be written to Kafka. */
    @Column(name = "payload", nullable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "attempts", nullable = false)
    private int attempts;

    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;

    public OutboxMessage(String aggregateType,
                         String aggregateId,
                         String eventId,
                         String topic,
                         String messageKey,
                         String payloadType,
                         String payload) {
        this.id = UUID.randomUUID();
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventId = eventId;
        this.topic = topic;
        this.messageKey = messageKey;
        this.payloadType = payloadType;
        this.payload = payload;
        this.createdAt = Instant.now();
        this.attempts = 0;
    }

    public void markPublished() {
        this.publishedAt = Instant.now();
        this.lastError = null;
    }

    public void markFailed(String error) {
        this.attempts++;
        this.lastError = error;
    }
}
