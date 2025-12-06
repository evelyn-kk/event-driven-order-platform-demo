package io.github.evelynkk.orderplatform.order;

import io.github.evelynkk.orderplatform.events.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = {"order.created"})
class OrderServiceIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private TestRestTemplate restTemplate;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> "${spring.embedded.kafka.brokers}");
    }

    @Test
    void shouldPublishOrderCreatedEventForHappyPath() {
        CreateOrderRequest request = new CreateOrderRequest(
                "user-test", "SKU-TEST", 2, new BigDecimal("199.00"));

        ResponseEntity<OrderCreatedEvent> response = restTemplate.postForEntity(
                "/orders", request, OrderCreatedEvent.class);

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        OrderCreatedEvent created = response.getBody();
        assertThat(created).isNotNull();
        assertThat(created.quantity()).isEqualTo(2);
        assertThat(created.totalAmount()).isEqualByComparingTo(new BigDecimal("199.00"));

        String message = consumeFirstMessage("order.created");
        assertThat(message).contains(created.orderId());
        assertThat(message).contains("SKU-TEST");
    }

    @Test
    void shouldSetHighQuantityForOutOfStockScenario() {
        CreateOrderRequest request = new CreateOrderRequest(
                "user-test", "SKU-TEST", 2, new BigDecimal("199.00"));

        ResponseEntity<OrderCreatedEvent> response = restTemplate.postForEntity(
                "/orders?scenario=OUT_OF_STOCK", request, OrderCreatedEvent.class);

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        OrderCreatedEvent created = response.getBody();
        assertThat(created).isNotNull();
        assertThat(created.quantity()).isEqualTo(999);

        String message = consumeFirstMessage("order.created");
        assertThat(message).contains(created.orderId());
    }

    @Test
    void shouldSetHighAmountForPaymentFailedScenario() {
        CreateOrderRequest request = new CreateOrderRequest(
                "user-test", "SKU-TEST", 2, new BigDecimal("199.00"));

        ResponseEntity<OrderCreatedEvent> response = restTemplate.postForEntity(
                "/orders?scenario=PAYMENT_FAILED", request, OrderCreatedEvent.class);

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        OrderCreatedEvent created = response.getBody();
        assertThat(created).isNotNull();
        assertThat(created.totalAmount()).isEqualByComparingTo(new BigDecimal("99999"));

        String message = consumeFirstMessage("order.created");
        assertThat(message).contains(created.orderId());
    }

    private String consumeFirstMessage(String topic) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(
                "test-consumer", "true", embeddedKafka);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(props);
        try (Consumer<String, String> consumer = factory.createConsumer()) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
            assertThat(records).hasSizeGreaterThan(0);
            return records.iterator().next().value();
        }
    }
}
