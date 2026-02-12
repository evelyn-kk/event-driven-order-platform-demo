package io.github.evelynkk.orderplatform.messaging.observability;

import io.github.evelynkk.orderplatform.messaging.outbox.OutboxRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;

/**
 * Health signals for the outbox.
 *
 * <p>Depth alone is a poor alarm: a burst of writes makes it spike even when the relay is keeping
 * up perfectly. Age of the oldest unpublished row is the one that matters — it stays near zero
 * while the relay drains and climbs without bound the moment it stops, which distinguishes "busy"
 * from "broken".
 */
@Component
@RequiredArgsConstructor
public class OutboxMetrics {

    private final OutboxRepository outbox;
    private final MeterRegistry registry;

    @jakarta.annotation.PostConstruct
    void register() {
        Gauge.builder("platform.outbox.backlog", this, OutboxMetrics::backlogSize)
                .description("Outbox rows written but not yet published to Kafka")
                .register(registry);

        Gauge.builder("platform.outbox.backlog.age", this, OutboxMetrics::backlogAgeSeconds)
                .description("Age of the oldest unpublished outbox row")
                .baseUnit("seconds")
                .register(registry);
    }

    @Transactional(readOnly = true)
    public double backlogSize() {
        return outbox.countByPublishedAtIsNull();
    }

    @Transactional(readOnly = true)
    public double backlogAgeSeconds() {
        Instant oldest = outbox.findOldestUnpublishedCreatedAt();
        return oldest == null ? 0d : Duration.between(oldest, Instant.now()).toMillis() / 1000d;
    }
}
