package io.github.evelynkk.orderplatform.messaging.dlq;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "platform.dlq")
public class DeadLetterProperties {

    /** Single dead-letter topic for the platform; the original topic travels in a header. */
    private String topic = "order.events.dlq";

    /** Delay before the first retry. */
    private long initialBackoffMs = 500;

    private double backoffMultiplier = 2.0;

    /**
     * Ceiling on any single wait. Retries block the partition, so an unbounded backoff would let
     * one bad record stall every order behind it.
     */
    private long maxBackoffMs = 8_000;

    /** Attempts after the first failure before the record is dead-lettered. */
    private int maxRetries = 4;

    /** Consumer group used by the admin endpoints when reading the dead-letter topic. */
    private String adminGroupId = "dlq-admin";
}
