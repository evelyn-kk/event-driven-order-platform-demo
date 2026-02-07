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

    /**
     * How long the producer waits to fill a batch before sending.
     *
     * <p>Zero sends every record on its own, paying a broker round trip per event. A few
     * milliseconds costs latency the outbox has already spent waiting for its poll tick, and buys
     * batching across the whole drain.
     */
    private int lingerMs = 5;

    /** Bytes per producer batch. Raising it lets a linger window pack more records per request. */
    private int batchSizeBytes = 64 * 1024;

    /** Events are JSON and compress well; lz4 trades a little CPU for a smaller network payload. */
    private String compressionType = "lz4";
}
