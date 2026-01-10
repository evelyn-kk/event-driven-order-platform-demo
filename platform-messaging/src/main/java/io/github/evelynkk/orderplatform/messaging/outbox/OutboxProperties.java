package io.github.evelynkk.orderplatform.messaging.outbox;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "platform.outbox")
public class OutboxProperties {

    /** How often the relay looks for unpublished rows, in milliseconds. */
    private long pollIntervalMs = 200;

    /** Rows claimed per pass. Larger batches amortise the round trip but hold row locks longer. */
    private int batchSize = 200;

    /** How long to wait for Kafka to acknowledge a batch before giving up and retrying next pass. */
    private long sendTimeoutMs = 10_000;

    /**
     * Cap on consecutive full batches drained in a single tick. Without it a sustained write burst
     * would keep one scheduler thread inside {@code drain()} indefinitely.
     */
    private int maxDrainRounds = 10;

    /** Attempt count past which a stuck row is logged as an error rather than a warning. */
    private int alertAfterAttempts = 5;
}
