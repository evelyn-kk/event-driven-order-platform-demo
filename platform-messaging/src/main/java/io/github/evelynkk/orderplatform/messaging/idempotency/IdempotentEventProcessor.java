package io.github.evelynkk.orderplatform.messaging.idempotency;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Turns Kafka's at-least-once delivery into effectively-once processing.
 *
 * <p>The business write and the "I have seen this event" marker commit in a single transaction.
 * Either both land or neither does, so there is no window in which work is done but unrecorded
 * (a redelivery would repeat it) or recorded but not done (a redelivery would skip it).
 *
 * <p>Note this is <em>effectively</em>-once, not exactly-once: the event may still be delivered
 * many times and the handler may still start many times. What is guaranteed is that its effect on
 * this service's state lands once.
 */
@Slf4j
@Component
public class IdempotentEventProcessor {

    private final ProcessedEventRepository repository;
    private final TransactionTemplate transactionTemplate;
    private final MeterRegistry registry;

    public IdempotentEventProcessor(ProcessedEventRepository repository,
                                    PlatformTransactionManager transactionManager,
                                    MeterRegistry registry) {
        this.repository = repository;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.registry = registry;
    }

    /**
     * Runs {@code work} unless this consumer group has already handled {@code eventId}.
     *
     * @return true if the work ran, false if it was skipped as a duplicate
     */
    public boolean processOnce(String eventId, String consumerGroup, Runnable work) {
        try {
            Boolean executed = transactionTemplate.execute(status -> {
                // Fast path only. Two concurrent deliveries of the same event can both pass this
                // check; the primary key below is what actually enforces the guarantee.
                if (repository.existsById(new ProcessedEventId(eventId, consumerGroup))) {
                    return false;
                }
                // Claim before working, so a concurrent duplicate fails on the constraint straight
                // away rather than after both have done the work.
                repository.saveAndFlush(new ProcessedEvent(eventId, consumerGroup));
                work.run();
                return true;
            });
            if (Boolean.FALSE.equals(executed)) {
                log.debug("Skipped duplicate delivery: eventId={}, group={}", eventId, consumerGroup);
                count(consumerGroup, "duplicate");
                return false;
            }
            count(consumerGroup, "applied");
            return true;
        } catch (DataIntegrityViolationException duplicate) {
            // Another thread or instance claimed the same event and committed first. Its
            // transaction did the work; ours rolled back cleanly. Nothing left to do.
            log.debug("Lost the race on a concurrent duplicate: eventId={}, group={}",
                    eventId, consumerGroup);
            count(consumerGroup, "duplicate");
            return false;
        }
    }

    /**
     * A steadily non-zero duplicate rate is worth seeing. It usually means consumers are being
     * rebalanced, or a producer is republishing — both invisible from throughput alone, because
     * duplicates look like healthy traffic until you separate them out.
     */
    private void count(String consumerGroup, String outcome) {
        Counter.builder("platform.events.processed")
                .description("Events handled, split by whether the work actually ran")
                .tag("group", consumerGroup)
                .tag("outcome", outcome)
                .register(registry)
                .increment();
    }
}
