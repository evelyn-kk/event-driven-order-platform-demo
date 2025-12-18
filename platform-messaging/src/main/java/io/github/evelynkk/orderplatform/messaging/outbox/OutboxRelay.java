package io.github.evelynkk.orderplatform.messaging.outbox;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Publishes committed outbox rows to Kafka.
 *
 * <p>The relay owns its own producer rather than reusing the application's {@code KafkaTemplate}.
 * Rows already hold the exact bytes the business transaction serialized, so re-parsing them into
 * domain objects only to have a {@code JsonSerializer} re-encode them would waste work and, worse,
 * let a later change to serializer configuration silently rewrite events that were already
 * committed. The relay stays a dumb pipe: bytes in, bytes out, with the original type carried in
 * the {@code __TypeId__} header so consumers still deserialize into the right class.
 */
@Slf4j
@Component
public class OutboxRelay {

    /** Header Spring Kafka's {@code JsonDeserializer} reads to pick a target type. */
    private static final String TYPE_ID_HEADER = "__TypeId__";
    private static final String EVENT_ID_HEADER = "eventId";

    private final OutboxRepository repository;
    private final OutboxProperties properties;
    private final TransactionTemplate transactionTemplate;
    private final DefaultKafkaProducerFactory<String, byte[]> producerFactory;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public OutboxRelay(OutboxRepository repository,
                       OutboxProperties properties,
                       KafkaProperties kafkaProperties,
                       ObjectProvider<KafkaConnectionDetails> connectionDetails,
                       PlatformTransactionManager transactionManager) {
        this.repository = repository;
        this.properties = properties;
        this.transactionTemplate = new TransactionTemplate(transactionManager);

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, resolveBootstrapServers(kafkaProperties, connectionDetails));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        // acks=all plus idempotence means a broker-side retry cannot duplicate or reorder a
        // record within its partition, so the only duplication left is the relay republishing
        // after a crash - which consumer-side dedup already covers.
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        this.producerFactory = new DefaultKafkaProducerFactory<>(config);
        this.kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }

    /**
     * Prefers {@link KafkaConnectionDetails} over the raw property.
     *
     * <p>Anything that supplies the broker address dynamically — Testcontainers via
     * {@code @ServiceConnection}, Docker Compose support, a service binding in a deployment —
     * contributes a connection-details bean and leaves {@code spring.kafka.bootstrap-servers} at
     * its default. Reading the property alone would silently point the relay at localhost while
     * every other Kafka client in the process talked to the real broker.
     */
    private static List<String> resolveBootstrapServers(KafkaProperties kafkaProperties,
                                                        ObjectProvider<KafkaConnectionDetails> connectionDetails) {
        KafkaConnectionDetails details = connectionDetails.getIfAvailable();
        return details != null ? details.getBootstrapServers() : kafkaProperties.getBootstrapServers();
    }

    @Scheduled(fixedDelayString = "${platform.outbox.poll-interval-ms:200}")
    public void drain() {
        for (int round = 0; round < properties.getMaxDrainRounds(); round++) {
            Integer claimed = transactionTemplate.execute(status -> publishBatch());
            // A short batch means the backlog is drained; stop early instead of spinning.
            if (claimed == null || claimed < properties.getBatchSize()) {
                return;
            }
        }
        log.debug("Outbox drain hit the per-tick round cap; backlog continues next tick");
    }

    private int publishBatch() {
        List<OutboxMessage> batch = repository.claimUnpublishedBatch(properties.getBatchSize());
        if (batch.isEmpty()) {
            return 0;
        }

        // Fire the whole batch before awaiting any acknowledgement. Sending and waiting one row at
        // a time would serialise a network round trip per message and cap throughput at roughly
        // one record per RTT regardless of batch size.
        List<CompletableFuture<SendResult<String, byte[]>>> inFlight = new ArrayList<>(batch.size());
        for (OutboxMessage message : batch) {
            inFlight.add(kafkaTemplate.send(toRecord(message)));
        }

        int published = 0;
        for (int i = 0; i < batch.size(); i++) {
            OutboxMessage message = batch.get(i);
            try {
                inFlight.get(i).get(properties.getSendTimeoutMs(), TimeUnit.MILLISECONDS);
                message.markPublished();
                published++;
            } catch (Exception e) {
                // Leave published_at null so the row is retried on a later pass. Dirty checking
                // persists the incremented attempt count when this transaction commits.
                message.markFailed(e.toString());
                if (message.getAttempts() >= properties.getAlertAfterAttempts()) {
                    log.error("Outbox row stuck after {} attempts: topic={}, eventId={}",
                            message.getAttempts(), message.getTopic(), message.getEventId(), e);
                } else {
                    log.warn("Failed to publish outbox row: topic={}, eventId={}",
                            message.getTopic(), message.getEventId(), e);
                }
            }
        }

        if (published > 0) {
            log.debug("Published {}/{} outbox rows", published, batch.size());
        }
        return batch.size();
    }

    private ProducerRecord<String, byte[]> toRecord(OutboxMessage message) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                message.getTopic(),
                null,
                message.getMessageKey(),
                message.getPayload().getBytes(StandardCharsets.UTF_8));
        record.headers().add(new RecordHeader(
                TYPE_ID_HEADER, message.getPayloadType().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(
                EVENT_ID_HEADER, message.getEventId().getBytes(StandardCharsets.UTF_8)));
        return record;
    }

    @PreDestroy
    void closeProducer() {
        producerFactory.destroy();
    }
}
