package io.github.evelynkk.orderplatform.order;

import io.github.evelynkk.orderplatform.events.DomainEvent;
import io.github.evelynkk.orderplatform.events.InventoryDeductedEvent;
import io.github.evelynkk.orderplatform.events.PaymentFailedEvent;
import io.github.evelynkk.orderplatform.messaging.dlq.DeadLetter;
import io.github.evelynkk.orderplatform.messaging.idempotency.ProcessedEventRepository;
import io.github.evelynkk.orderplatform.messaging.outbox.OutboxRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Exercises order-service against real Postgres and Kafka rather than embedded substitutes,
 * because the behaviour under test is precisely the interaction between them: a transaction that
 * spans a business write and an outbox row, and a deduplication guarantee that rests on a database
 * constraint.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class OrderSagaIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    private static KafkaTemplate<String, Object> upstream;

    /** Writes plain strings, used to plant a record the consumer cannot deserialize. */
    private static KafkaTemplate<String, String> poison;

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private OrderRepository orders;

    @Autowired
    private OutboxRepository outbox;

    @Autowired
    private ProcessedEventRepository processedEvents;

    @BeforeEach
    void setUpProducer() {
        if (upstream == null) {
            Map<String, Object> config = new HashMap<>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            // JsonSerializer stamps __TypeId__, matching what the outbox relay emits in production.
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            upstream = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));

            Map<String, Object> plain = new HashMap<>(config);
            plain.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            poison = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(plain));
        }
    }

    @AfterAll
    static void closeProducer() {
        if (upstream != null) {
            upstream.destroy();
        }
        if (poison != null) {
            poison.destroy();
        }
    }

    @Test
    void placingAnOrderCommitsTheOrderAndItsEventTogether() {
        String orderId = placeOrder(OrderController.Scenario.HAPPY);

        Order stored = orders.findById(orderId).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(OrderStatus.CREATED);

        // The relay drains asynchronously, so the row lands first and the Kafka record follows.
        // Wait for this order specifically: other tests in the class publish to the same topic,
        // so "some record arrived" would pass without proving anything about this one.
        await().atMost(Duration.ofSeconds(30)).until(
                () -> consume("order.created"),
                records -> records.stream().anyMatch(payload -> payload.contains(orderId)));

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(outbox.countByPublishedAtIsNull()).isZero());
    }

    @Test
    void reservedInventoryAdvancesTheOrder() {
        String orderId = placeOrder(OrderController.Scenario.HAPPY);

        upstream.send("inventory.deducted", orderId, new InventoryDeductedEvent(
                DomainEvent.newEventId(), orderId, "SKU-1001", 2, 98, Instant.now()));

        awaitStatus(orderId, OrderStatus.INVENTORY_RESERVED);
    }

    @Test
    void redeliveringTheSameEventAppliesItOnlyOnce() {
        String orderId = placeOrder(OrderController.Scenario.HAPPY);

        // One event, delivered twice - exactly what a consumer sees after a rebalance or a retry.
        InventoryDeductedEvent reserved = new InventoryDeductedEvent(
                DomainEvent.newEventId(), orderId, "SKU-1001", 2, 98, Instant.now());
        upstream.send("inventory.deducted", orderId, reserved);
        upstream.send("inventory.deducted", orderId, reserved);

        awaitStatus(orderId, OrderStatus.INVENTORY_RESERVED);

        // A second application would have been recorded as a second marker for this group.
        long markers = processedEvents.findAll().stream()
                .filter(marker -> marker.getEventId().equals(reserved.eventId()))
                .count();
        assertThat(markers).isEqualTo(1);
    }

    @Test
    void failedPaymentCancelsTheOrderAndAnnouncesIt() {
        String orderId = placeOrder(OrderController.Scenario.HAPPY);

        upstream.send("inventory.deducted", orderId, new InventoryDeductedEvent(
                DomainEvent.newEventId(), orderId, "SKU-1001", 2, 98, Instant.now()));
        awaitStatus(orderId, OrderStatus.INVENTORY_RESERVED);

        upstream.send("payment.failed", orderId, new PaymentFailedEvent(
                DomainEvent.newEventId(), orderId, "user-001",
                new BigDecimal("99999.00"), "AMOUNT_EXCEEDS_LIMIT", Instant.now()));

        awaitStatus(orderId, OrderStatus.CANCELLED);
        assertThat(orders.findById(orderId).orElseThrow().getCancelReason())
                .startsWith("PAYMENT_FAILED");

        // Cancellation has to reach the other participants; that is what releases the stock.
        await().atMost(Duration.ofSeconds(30)).until(
                () -> consume("order.cancelled"),
                records -> records.stream().anyMatch(payload -> payload.contains(orderId)));
    }

    @Test
    void aLateSuccessEventCannotResurrectACancelledOrder() {
        String orderId = placeOrder(OrderController.Scenario.HAPPY);

        upstream.send("payment.failed", orderId, new PaymentFailedEvent(
                DomainEvent.newEventId(), orderId, "user-001",
                new BigDecimal("99999.00"), "AMOUNT_EXCEEDS_LIMIT", Instant.now()));
        awaitStatus(orderId, OrderStatus.CANCELLED);

        // A service that was already mid-flight when the cancellation was decided reports success.
        upstream.send("inventory.deducted", orderId, new InventoryDeductedEvent(
                DomainEvent.newEventId(), orderId, "SKU-1001", 2, 98, Instant.now()));

        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(orders.findById(orderId).orElseThrow().getStatus())
                        .isEqualTo(OrderStatus.CANCELLED));
    }

    @Test
    void anEventForAnUnknownOrderIsDeadLetteredRatherThanRetriedForever() {
        // No order was ever placed with this id, and order-service writes the order in the same
        // transaction that announces it - so this can never become valid, however long we retry.
        String phantomOrderId = "phantom-" + DomainEvent.newEventId();

        upstream.send("inventory.deducted", phantomOrderId, new InventoryDeductedEvent(
                DomainEvent.newEventId(), phantomOrderId, "SKU-1001", 1, 99, Instant.now()));

        DeadLetter dead = awaitDeadLetterFor(phantomOrderId);
        assertThat(dead.originalTopic()).isEqualTo("inventory.deducted");
        assertThat(dead.rootCauseType()).contains("PermanentEventException");
    }

    @Test
    void aMalformedRecordIsDeadLetteredInsteadOfStallingThePartition() {
        String marker = "not-json-" + DomainEvent.newEventId();

        // No __TypeId__ header and a body that is not an event at all: deserialization fails
        // before any handler runs. Without ErrorHandlingDeserializer this would throw inside the
        // poll loop and the container would spin on this offset forever.
        poison.send("inventory.deducted", "poison-key", marker);

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(rest.getForObject("/admin/dlq?limit=200", DeadLetter[].class))
                        .anyMatch(letter -> letter.payload() != null && letter.payload().contains(marker)));
    }

    @Test
    void deadLettersCanBeReplayedBackOntoTheirOriginalTopic() {
        String phantomOrderId = "replay-" + DomainEvent.newEventId();

        upstream.send("inventory.deducted", phantomOrderId, new InventoryDeductedEvent(
                DomainEvent.newEventId(), phantomOrderId, "SKU-1001", 1, 99, Instant.now()));
        awaitDeadLetterFor(phantomOrderId);

        Map<?, ?> result = rest.postForObject("/admin/dlq/replay?limit=100", null, Map.class);

        // Replay only reports what it moved; whether the records then succeed depends on whether
        // the cause was actually fixed, which is the operator's problem, not the endpoint's.
        assertThat((Integer) result.get("replayed")).isPositive();
    }

    @Test
    void invalidRequestsAreRejectedAtTheEdge() {
        ResponseEntity<String> response = rest.postForEntity(
                "/orders",
                new CreateOrderRequest("user-001", "SKU-1001", 0, new BigDecimal("199.00")),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    private String placeOrder(OrderController.Scenario scenario) {
        ResponseEntity<OrderView> response = rest.postForEntity(
                "/orders?scenario=" + scenario,
                new CreateOrderRequest("user-001", "SKU-1001", 2, new BigDecimal("199.00")),
                OrderView.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        return response.getBody().orderId();
    }

    private DeadLetter awaitDeadLetterFor(String orderId) {
        return await().atMost(Duration.ofSeconds(60)).until(
                () -> Arrays.stream(rest.getForObject("/admin/dlq?limit=200", DeadLetter[].class))
                        .filter(letter -> orderId.equals(letter.key()))
                        .findFirst()
                        .orElse(null),
                Objects::nonNull);
    }

    private void awaitStatus(String orderId, OrderStatus expected) {
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(orders.findById(orderId).orElseThrow().getStatus())
                        .isEqualTo(expected));
    }

    /** Reads a topic from the beginning with a throwaway group, so tests never share offsets. */
    private List<String> consume(String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "assertions-" + DomainEvent.newEventId());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<String> payloads = new ArrayList<>();
        try (Consumer<String, String> consumer =
                     new DefaultKafkaConsumerFactory<String, String>(config).createConsumer()) {
            consumer.subscribe(List.of(topic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                payloads.add(record.value());
            }
        }
        return payloads;
    }
}
