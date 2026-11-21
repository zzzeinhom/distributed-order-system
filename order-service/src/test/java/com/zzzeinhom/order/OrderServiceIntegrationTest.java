package com.zzzeinhom.order;

import com.zzzeinhom.order.dto.CreateOrderRequest;
import com.zzzeinhom.order.dto.OrderCreatedEvent;
import com.zzzeinhom.order.dto.OrderResponse;
import com.zzzeinhom.order.entity.OrderStatus;
import com.zzzeinhom.order.repository.OrderRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class OrderServiceIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    @Container
    @ServiceConnection(name = "redis")
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    private static KafkaConsumer<String, OrderCreatedEvent> testKafkaConsumer;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @BeforeAll
    static void setupKafkaConsumer() {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-verifier-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class,
                JsonDeserializer.TRUSTED_PACKAGES, "com.zzzeinhom.*",
                JsonDeserializer.VALUE_DEFAULT_TYPE, OrderCreatedEvent.class.getName(),
                JsonDeserializer.USE_TYPE_INFO_HEADERS, false
        );
        testKafkaConsumer = new KafkaConsumer<>(props);
        testKafkaConsumer.subscribe(Collections.singletonList("order-events"));
    }

    @AfterAll
    static void tearDown() {
        if (testKafkaConsumer != null) {
            testKafkaConsumer.close();
        }
    }

    @Test
    void shouldCreateOrderPublishOutboxEventAndEnforceIdempotencyAcrossRedisAndDb() {
        String idempotencyKey = "test-idem-" + UUID.randomUUID();
        CreateOrderRequest request = new CreateOrderRequest(
                UUID.randomUUID(),
                UUID.randomUUID(),
                2,
                new BigDecimal("250.00")
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", idempotencyKey);
        HttpEntity<CreateOrderRequest> entity = new HttpEntity<>(request, headers);

        // 1. First request -> 202 Accepted + persisted in PostgreSQL
        ResponseEntity<OrderResponse> firstResponse = restTemplate.postForEntity("/api/v1/orders", entity, OrderResponse.class);
        assertThat(firstResponse.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(firstResponse.getBody()).isNotNull();
        UUID orderId = firstResponse.getBody().orderId();
        assertThat(firstResponse.getBody().status()).isEqualTo(OrderStatus.PENDING);

        // 2. Verify OutboxRelay publishes OrderCreatedEvent to Kafka topic
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var records = testKafkaConsumer.poll(Duration.ofMillis(200));
            assertThat(records.count()).isGreaterThanOrEqualTo(1);
            var event = records.iterator().next().value();
            assertThat(event.orderId()).isEqualTo(orderId);
            assertThat(event.quantity()).isEqualTo(2);
        });

        // 3. Duplicate request with same Idempotency-Key -> returns 200 OK from Redis with identical orderId
        ResponseEntity<OrderResponse> cachedResponse = restTemplate.postForEntity("/api/v1/orders", entity, OrderResponse.class);
        assertThat(cachedResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(cachedResponse.getBody().orderId()).isEqualTo(orderId);

        // 4. Simulate Redis eviction/crash -> PostgreSQL unique constraint prevents duplicate order
        redisTemplate.delete("idempotency:order:" + idempotencyKey);
        ResponseEntity<OrderResponse> dbFallbackResponse = restTemplate.postForEntity("/api/v1/orders", entity, OrderResponse.class);
        assertThat(dbFallbackResponse.getBody().orderId()).isEqualTo(orderId);
        assertThat(orderRepository.count()).isEqualTo(1);
    }
}
