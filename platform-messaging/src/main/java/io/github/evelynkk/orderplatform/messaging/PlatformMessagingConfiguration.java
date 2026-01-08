package io.github.evelynkk.orderplatform.messaging;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.evelynkk.orderplatform.messaging.outbox.OutboxProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Wiring shared by every service that publishes through the outbox or consumes idempotently.
 *
 * <p>Services opt in by scanning {@code io.github.evelynkk.orderplatform.messaging} alongside
 * their own package.
 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties(OutboxProperties.class)
public class PlatformMessagingConfiguration {

    /**
     * Dedicated mapper for outbox payloads, deliberately independent of the application's own
     * {@code ObjectMapper}. Outbox rows are a wire format: once a row is committed its bytes are
     * fixed, and an unrelated tweak to the app's Jackson configuration must not change how
     * already-queued events are encoded.
     *
     * <p>Timestamps are written as ISO-8601 rather than epoch numbers so that a message sitting in
     * Kafka UI or a DLQ is readable during an incident.
     */
    @Bean("outboxObjectMapper")
    public ObjectMapper outboxObjectMapper() {
        return JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .build();
    }
}
