package io.github.evelynkk.orderplatform.inventory;

import io.github.evelynkk.orderplatform.events.DomainEvent;
import io.github.evelynkk.orderplatform.events.OrderCancelledEvent;
import io.github.evelynkk.orderplatform.events.OrderCreatedEvent;
import io.github.evelynkk.orderplatform.events.ShippingCreatedEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Reserving stock is the least forgiving step in the saga: it is not naturally idempotent, and a
 * hold that is taken twice or released never is a permanent loss of sellable inventory. These
 * tests pin down both directions.
 */
@SpringBootTest
@Testcontainers
class InventoryReservationIntegrationTest {

    private static final String IN_STOCK = "SKU-1001";
    private static final String OUT_OF_STOCK = "SKU-1005";

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    private static KafkaTemplate<String, Object> upstream;

    @Autowired
    private InventoryItemRepository items;

    @Autowired
    private StockReservationRepository reservations;

    @BeforeEach
    void setUpProducer() {
        if (upstream == null) {
            Map<String, Object> config = new HashMap<>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            upstream = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
        }
    }

    @AfterAll
    static void closeProducer() {
        if (upstream != null) {
            upstream.destroy();
        }
    }

    @Test
    void reservingHoldsStockWithoutDestroyingIt() {
        int before = available(IN_STOCK);
        String orderId = newOrderId();

        upstream.send("order.created", orderId, orderCreated(orderId, IN_STOCK, 3));

        awaitReservationState(orderId, StockReservation.State.RESERVED);
        // Held, not gone: available drops by three and reserved rises by the same amount, so the
        // total is conserved and an unreleased hold is visible rather than silently absorbed.
        assertThat(available(IN_STOCK)).isEqualTo(before - 3);
        assertThat(reserved(IN_STOCK)).isGreaterThanOrEqualTo(3);
    }

    @Test
    void aRedeliveredOrderReservesStockOnlyOnce() {
        int before = available(IN_STOCK);
        String orderId = newOrderId();

        OrderCreatedEvent event = orderCreated(orderId, IN_STOCK, 4);
        upstream.send("order.created", orderId, event);
        upstream.send("order.created", orderId, event);

        awaitReservationState(orderId, StockReservation.State.RESERVED);

        // Without deduplication the second delivery would take another four units that no
        // compensation would ever give back, because only one order.cancelled can arrive.
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(available(IN_STOCK)).isEqualTo(before - 4));
        assertThat(reservations.findById(orderId).orElseThrow().getQuantity()).isEqualTo(4);
    }

    @Test
    void insufficientStockLeavesInventoryUntouched() {
        int before = available(OUT_OF_STOCK);
        String orderId = newOrderId();

        upstream.send("order.created", orderId, orderCreated(orderId, OUT_OF_STOCK, 1));

        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(available(OUT_OF_STOCK)).isEqualTo(before));
        assertThat(reservations.findById(orderId)).isEmpty();
    }

    @Test
    void cancellationReleasesTheHold() {
        int before = available(IN_STOCK);
        String orderId = newOrderId();

        upstream.send("order.created", orderId, orderCreated(orderId, IN_STOCK, 5));
        awaitReservationState(orderId, StockReservation.State.RESERVED);

        upstream.send("order.cancelled", orderId, new OrderCancelledEvent(
                DomainEvent.newEventId(), orderId, "PAYMENT_FAILED", Instant.now()));

        awaitReservationState(orderId, StockReservation.State.RELEASED);
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(available(IN_STOCK)).isEqualTo(before));
    }

    @Test
    void aRedeliveredCancellationDoesNotRefundTwice() {
        int before = available(IN_STOCK);
        String orderId = newOrderId();

        upstream.send("order.created", orderId, orderCreated(orderId, IN_STOCK, 6));
        awaitReservationState(orderId, StockReservation.State.RESERVED);

        OrderCancelledEvent cancelled = new OrderCancelledEvent(
                DomainEvent.newEventId(), orderId, "PAYMENT_FAILED", Instant.now());
        upstream.send("order.cancelled", orderId, cancelled);
        upstream.send("order.cancelled", orderId, cancelled);

        awaitReservationState(orderId, StockReservation.State.RELEASED);
        // A second release would invent six units of stock that were never in the warehouse.
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(available(IN_STOCK)).isEqualTo(before));
    }

    @Test
    void shippingConsumesTheHoldRatherThanReturningIt() {
        int before = available(IN_STOCK);
        String orderId = newOrderId();

        upstream.send("order.created", orderId, orderCreated(orderId, IN_STOCK, 2));
        awaitReservationState(orderId, StockReservation.State.RESERVED);
        int reservedAfterHold = reserved(IN_STOCK);

        upstream.send("shipping.created", orderId, new ShippingCreatedEvent(
                DomainEvent.newEventId(), orderId, UUID.randomUUID().toString(),
                "SF-Express", "SF123456", Instant.now()));

        awaitReservationState(orderId, StockReservation.State.COMMITTED);
        // The goods left the warehouse, so the hold is consumed: reserved falls, available does
        // not recover.
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(reserved(IN_STOCK)).isEqualTo(reservedAfterHold - 2));
        assertThat(available(IN_STOCK)).isEqualTo(before - 2);
    }

    private static String newOrderId() {
        return UUID.randomUUID().toString();
    }

    private static OrderCreatedEvent orderCreated(String orderId, String productId, int quantity) {
        return new OrderCreatedEvent(
                DomainEvent.newEventId(), orderId, "user-001", productId,
                quantity, new BigDecimal("199.00"), Instant.now());
    }

    private void awaitReservationState(String orderId, StockReservation.State expected) {
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(reservations.findById(orderId))
                        .get()
                        .extracting(StockReservation::getState)
                        .isEqualTo(expected));
    }

    private int available(String productId) {
        return items.findById(productId).orElseThrow().getAvailable();
    }

    private int reserved(String productId) {
        return items.findById(productId).orElseThrow().getReserved();
    }
}
