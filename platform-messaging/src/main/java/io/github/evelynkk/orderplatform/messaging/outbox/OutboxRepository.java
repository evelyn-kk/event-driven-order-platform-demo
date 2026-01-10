package io.github.evelynkk.orderplatform.messaging.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxMessage, UUID> {

    /**
     * Claims a batch of unpublished messages for this relay instance.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} is what makes the relay horizontally scalable: rows locked
     * by another instance are stepped over rather than waited on, so N relays drain N disjoint
     * batches in parallel without a coordinator and without blocking each other.
     *
     * <p>Ordering by {@code created_at} keeps publication roughly in causal order. Per-order
     * ordering on the topic itself comes from the partition key, not from this query.
     */
    @Query(value = """
            SELECT * FROM outbox_message
            WHERE published_at IS NULL
            ORDER BY created_at
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxMessage> claimUnpublishedBatch(@Param("batchSize") int batchSize);

    long countByPublishedAtIsNull();

    /** Backlog age, used as the outbox health signal: how long the oldest unpublished row has waited. */
    @Query("SELECT MIN(m.createdAt) FROM OutboxMessage m WHERE m.publishedAt IS NULL")
    Instant findOldestUnpublishedCreatedAt();
}
