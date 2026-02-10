package io.github.evelynkk.orderplatform.messaging.dlq;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Retry and dead-letter policy for every listener in the process.
 *
 * <p>Spring Boot hands any {@code CommonErrorHandler} bean to the listener container factory, so
 * defining one here applies it to all {@code @KafkaListener} methods without each service
 * repeating the wiring.
 */
@Configuration
@EnableConfigurationProperties(DeadLetterProperties.class)
public class KafkaErrorHandlingConfiguration {

    /**
     * Producer used only to move failed records aside.
     *
     * <p>{@link DelegatingByTypeSerializer} is needed because the two failure modes hand the
     * recoverer different things: a handler that threw leaves a deserialized event object, while a
     * record that could not be deserialized at all leaves the raw bytes. One serializer cannot
     * cover both, and a dead-letter path that itself fails to serialize loses the evidence.
     */
    @Bean
    public KafkaTemplate<Object, Object> deadLetterKafkaTemplate(
            KafkaProperties kafkaProperties,
            ObjectProvider<KafkaConnectionDetails> connectionDetails) {

        KafkaConnectionDetails details = connectionDetails.getIfAvailable();
        List<String> bootstrapServers = details != null
                ? details.getBootstrapServers()
                : kafkaProperties.getBootstrapServers();

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Assignability matching (the second argument) is required: without it the lookup is by
        // exact class, a concrete event type matches neither entry, and the dead-letter send fails
        // — leaving the error handler retrying a record it can never set aside.
        //
        // It also makes iteration order load-bearing, because Object matches everything. A
        // LinkedHashMap puts byte[] first so raw bytes from a failed deserialization are written
        // through unchanged; letting Jackson claim them would base64-encode the very payload an
        // operator needs to read.
        Map<Class<?>, org.apache.kafka.common.serialization.Serializer<?>> delegates =
                new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(Object.class, new JsonSerializer<>());
        DelegatingByTypeSerializer valueSerializer = new DelegatingByTypeSerializer(delegates, true);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<Object, Object>(
                config, null, valueSerializer));
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> deadLetterKafkaTemplate,
                                                 DeadLetterProperties properties) {
        // Every topic funnels into one dead-letter topic. The recoverer stamps the original topic,
        // partition, offset, exception type and stack trace into headers, so a single place to
        // look during an incident beats one dead-letter topic per source topic.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                deadLetterKafkaTemplate,
                (record, exception) -> new TopicPartition(properties.getTopic(), -1));

        ExponentialBackOff backOff = new ExponentialBackOff(
                properties.getInitialBackoffMs(), properties.getBackoffMultiplier());
        backOff.setMaxInterval(properties.getMaxBackoffMs());
        backOff.setMaxElapsedTime(totalRetryBudgetMs(properties));

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // Retries here are blocking: the container sleeps on the consumer thread and the partition
        // makes no progress. That is the right trade for this platform, because all events for one
        // order share a partition and non-blocking retry topics would let a later event overtake
        // the one being retried. The cost is bounded by the backoff ceiling above.
        handler.addNotRetryableExceptions(PermanentEventException.class);
        handler.setCommitRecovered(true);

        return handler;
    }

    /**
     * Converts a retry count into the elapsed-time budget {@link ExponentialBackOff} works in,
     * summing the capped intervals rather than assuming uncapped growth.
     */
    private static long totalRetryBudgetMs(DeadLetterProperties properties) {
        long interval = properties.getInitialBackoffMs();
        long total = 0;
        for (int attempt = 0; attempt < properties.getMaxRetries(); attempt++) {
            total += interval;
            interval = Math.min((long) (interval * properties.getBackoffMultiplier()),
                    properties.getMaxBackoffMs());
        }
        return total;
    }
}
