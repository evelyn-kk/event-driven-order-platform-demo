package io.github.evelynkk.orderplatform.messaging.dlq;

import java.time.Instant;

/**
 * A dead-lettered record, flattened for triage.
 *
 * <p>{@code originalTopic} and {@code originalOffset} come from the headers the recoverer stamps,
 * and are what make replay possible: the dead-letter topic knows where each record came from.
 */
public record DeadLetter(
        String originalTopic,
        Integer originalPartition,
        Long originalOffset,
        /** Always the listener wrapper; kept because it identifies where the failure was caught. */
        String exceptionType,
        /** What actually went wrong. This is the field worth reading first during triage. */
        String rootCauseType,
        String exceptionMessage,
        String key,
        String payload,
        Instant deadLetteredAt,
        long dlqOffset
) {
}
