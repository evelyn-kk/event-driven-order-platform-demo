package io.github.evelynkk.orderplatform.messaging.dlq;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Triage and recovery for dead-lettered records.
 *
 * <p>A dead-letter topic nobody can read or drain is just a slower way of dropping messages. This
 * gives the two operations an incident actually needs: see what failed and why, then put the
 * records back once the cause is fixed.
 */
@Slf4j
@Service
public class DeadLetterAdminService {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(2);

    private final DeadLetterProperties properties;
    private final KafkaTemplate<Object, Object> republishTemplate;
    private final List<String> bootstrapServers;

    public DeadLetterAdminService(DeadLetterProperties properties,
                                  KafkaTemplate<Object, Object> deadLetterKafkaTemplate,
                                  KafkaProperties kafkaProperties,
                                  ObjectProvider<KafkaConnectionDetails> connectionDetails) {
        this.properties = properties;
        this.republishTemplate = deadLetterKafkaTemplate;
        KafkaConnectionDetails details = connectionDetails.getIfAvailable();
        this.bootstrapServers = details != null
                ? details.getBootstrapServers()
                : kafkaProperties.getBootstrapServers();
    }

    /**
     * Reads dead letters without consuming them.
     *
     * <p>Uses a throwaway group and an explicit assignment from the beginning, so inspecting the
     * topic never moves the offsets that {@link #replay(int)} depends on.
     */
    public List<DeadLetter> peek(int limit) {
        try (Consumer<String, byte[]> consumer = createConsumer("peek-" + UUID.randomUUID())) {
            List<TopicPartition> partitions = partitionsOf(consumer);
            if (partitions.isEmpty()) {
                return List.of();
            }
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            return drain(consumer, limit).stream().map(DeadLetterAdminService::toDeadLetter).toList();
        }
    }

    /**
     * Republishes dead letters to the topics they came from.
     *
     * <p>Committed offsets advance only after the records are back on their original topics, so an
     * interrupted replay repeats records rather than skipping them — the consumers on the far end
     * deduplicate on {@code eventId} anyway.
     *
     * @return how many records were replayed
     */
    public int replay(int limit) {
        try (Consumer<String, byte[]> consumer = createConsumer(properties.getAdminGroupId())) {
            consumer.subscribe(List.of(properties.getTopic()));
            List<ConsumerRecord<String, byte[]>> records = drain(consumer, limit);
            if (records.isEmpty()) {
                return 0;
            }

            for (ConsumerRecord<String, byte[]> record : records) {
                republish(record);
            }
            republishTemplate.flush();
            consumer.commitSync();

            log.info("Replayed {} dead-lettered records", records.size());
            return records.size();
        }
    }

    private void republish(ConsumerRecord<String, byte[]> record) {
        String originalTopic = stringHeader(record, KafkaHeaders.DLT_ORIGINAL_TOPIC);
        if (originalTopic == null) {
            throw new IllegalStateException(
                    "Dead letter at offset " + record.offset() + " has no original-topic header");
        }

        ProducerRecord<Object, Object> replayed =
                new ProducerRecord<>(originalTopic, record.key(), record.value());

        // Carry the payload's own headers across - __TypeId__ in particular, without which the
        // consumer cannot tell what it is deserializing - but drop the kafka_dlt-* forensics so
        // the replayed record is indistinguishable from the original.
        for (Header header : record.headers()) {
            if (!header.key().startsWith("kafka_dlt-")) {
                replayed.headers().add(header);
            }
        }

        republishTemplate.send(replayed);
    }

    private List<ConsumerRecord<String, byte[]>> drain(Consumer<String, byte[]> consumer, int limit) {
        List<ConsumerRecord<String, byte[]>> collected = new ArrayList<>();
        // Poll until a pass yields nothing: the first poll of a fresh assignment often returns
        // empty while the group settles, so a single poll would under-report.
        for (int emptyPasses = 0; collected.size() < limit && emptyPasses < 2; ) {
            ConsumerRecords<String, byte[]> batch = consumer.poll(POLL_TIMEOUT);
            if (batch.isEmpty()) {
                emptyPasses++;
                continue;
            }
            emptyPasses = 0;
            for (ConsumerRecord<String, byte[]> record : batch) {
                collected.add(record);
                if (collected.size() == limit) {
                    break;
                }
            }
        }
        return collected;
    }

    private List<TopicPartition> partitionsOf(Consumer<String, byte[]> consumer) {
        List<PartitionInfo> info = consumer.partitionsFor(properties.getTopic());
        if (info == null) {
            return List.of();
        }
        return info.stream()
                .map(partition -> new TopicPartition(partition.topic(), partition.partition()))
                .toList();
    }

    private Consumer<String, byte[]> createConsumer(String groupId) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Raw bytes, so a record is replayed exactly as it was rather than round-tripped through a
        // deserializer that may be the very thing that rejected it.
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new DefaultKafkaConsumerFactory<String, byte[]>(config).createConsumer();
    }

    private static DeadLetter toDeadLetter(ConsumerRecord<String, byte[]> record) {
        return new DeadLetter(
                stringHeader(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                numericHeader(record, KafkaHeaders.DLT_ORIGINAL_PARTITION, Integer.BYTES),
                numericHeader(record, KafkaHeaders.DLT_ORIGINAL_OFFSET, Long.BYTES),
                stringHeader(record, KafkaHeaders.DLT_EXCEPTION_FQCN),
                stringHeader(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN),
                stringHeader(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                record.key(),
                record.value() == null ? null : new String(record.value(), StandardCharsets.UTF_8),
                Instant.ofEpochMilli(record.timestamp()),
                record.offset());
    }

    private static String stringHeader(ConsumerRecord<String, byte[]> record, String name) {
        byte[] value = rawHeader(record, name);
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    /**
     * The partition and offset headers hold raw big-endian integers, not text, so they have to be
     * read as bytes — decoding them as UTF-8 first would mangle any byte outside ASCII.
     */
    private static <T extends Number> T numericHeader(ConsumerRecord<String, byte[]> record,
                                                      String name, int width) {
        byte[] value = rawHeader(record, name);
        if (value == null || value.length != width) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(value);
        @SuppressWarnings("unchecked")
        T result = (T) (width == Integer.BYTES
                ? (Number) buffer.getInt()
                : (Number) buffer.getLong());
        return result;
    }

    private static byte[] rawHeader(ConsumerRecord<String, byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : header.value();
    }
}
