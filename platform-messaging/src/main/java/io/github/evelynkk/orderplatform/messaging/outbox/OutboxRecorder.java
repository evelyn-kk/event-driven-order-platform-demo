package io.github.evelynkk.orderplatform.messaging.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.evelynkk.orderplatform.events.DomainEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Appends an event to the outbox as part of the caller's business transaction.
 *
 * <p>Callers must already be inside a transaction — {@link Propagation#MANDATORY} turns "someone
 * forgot the {@code @Transactional}" from a silent consistency bug into an immediate failure.
 * Without an enclosing transaction the outbox row would commit on its own and the whole point of
 * the pattern would be lost.
 */
@Slf4j
@Component
public class OutboxRecorder {

    private final OutboxRepository repository;
    private final ObjectMapper objectMapper;

    public OutboxRecorder(OutboxRepository repository,
                          @Qualifier("outboxObjectMapper") ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void record(String aggregateType, String topic, DomainEvent event) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            // Serialization failure means the event contract itself is broken; failing the
            // business transaction is correct — we must not commit state we cannot announce.
            throw new IllegalStateException(
                    "Unable to serialize " + event.getClass().getName() + " for the outbox", e);
        }

        repository.save(new OutboxMessage(
                aggregateType,
                event.orderId(),
                event.eventId(),
                topic,
                event.orderId(),
                event.getClass().getName(),
                payload));

        log.debug("Recorded {} to outbox for orderId={}, eventId={}",
                topic, event.orderId(), event.eventId());
    }
}
