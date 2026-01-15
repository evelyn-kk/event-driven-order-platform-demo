package io.github.evelynkk.orderplatform.messaging.idempotency;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Composite primary key for {@link ProcessedEvent}.
 *
 * <p>A plain class rather than a record: Hibernate 6.4 cannot introspect record components when
 * binding an {@code @IdClass} and fails at startup.
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class ProcessedEventId implements Serializable {

    private String eventId;

    private String consumerGroup;
}
