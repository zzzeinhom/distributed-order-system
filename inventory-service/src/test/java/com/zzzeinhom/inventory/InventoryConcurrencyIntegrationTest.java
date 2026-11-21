package com.zzzeinhom.inventory;

import com.zzzeinhom.inventory.dto.InventoryResultEvent;
import com.zzzeinhom.inventory.dto.OrderCreatedEvent;
import com.zzzeinhom.inventory.entity.ProductInventory;
import com.zzzeinhom.inventory.repository.ProductInventoryRepository;
import com.zzzeinhom.inventory.service.InventoryReservationCoordinator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
class InventoryConcurrencyIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    private static KafkaConsumer<String, InventoryResultEvent> resultConsumer;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ProductInventoryRepository inventoryRepository;

    @Autowired
    private InventoryReservationCoordinator coordinator;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeAll
    static void setupConsumer() {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "inventory-test-verifier",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class,
                JsonDeserializer.TRUSTED_PACKAGES, "com.zzzeinhom.*",
                JsonDeserializer.VALUE_DEFAULT_TYPE, InventoryResultEvent.class.getName(),
                JsonDeserializer.USE_TYPE_INFO_HEADERS, false
        );
        resultConsumer = new KafkaConsumer<>(props);
        resultConsumer.subscribe(Collections.singletonList("inventory-events"));
    }

    @AfterAll
    static void tearDown() {
        if (resultConsumer != null) {
            resultConsumer.close();
        }
    }

    @Test
    void shouldConsumeOrderEventOnceAndEnforceIdempotency() {
        UUID productId = UUID.randomUUID();
        jdbcTemplate.update(
                "INSERT INTO inventory_schema.inventory (product_id, sku, available_quantity, version) VALUES (?, ?, ?, 0)",
                productId, "SKU-" + productId.toString().substring(0, 8), 5
        );

        UUID sharedEventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = new OrderCreatedEvent(
                sharedEventId,
                orderId,
                UUID.randomUUID(),
                productId,
                2,
                new BigDecimal("100.00"),
                Instant.now()
        );

        // Publish duplicate event twice to simulate at-least-once Kafka delivery
        kafkaTemplate.send("order-events", productId.toString(), event);
        kafkaTemplate.send("order-events", productId.toString(), event);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ProductInventory updated = inventoryRepository.findById(productId).orElseThrow();
            assertThat(updated.getAvailableQuantity()).isEqualTo(3);

            var records = resultConsumer.poll(Duration.ofMillis(200));
            assertThat(records.count()).isEqualTo(1);
            assertThat(records.iterator().next().value().reserved()).isTrue();
        });
    }

    @Test
    void shouldPreventOversellingUnderConcurrentRequestsViaOptimisticLocking() throws Exception {
        UUID productId = UUID.randomUUID();
        jdbcTemplate.update(
                "INSERT INTO inventory_schema.inventory (product_id, sku, available_quantity, version) VALUES (?, ?, ?, 0)",
                productId, "FLASH-" + productId.toString().substring(0, 8), 3
        );

        int concurrentThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(concurrentThreads);
        CountDownLatch readyLatch = new CountDownLatch(concurrentThreads);
        CountDownLatch startLatch = new CountDownLatch(1);

        List<Future<Optional<InventoryResultEvent>>> futures = new ArrayList<>();

        for (int i = 0; i < concurrentThreads; i++) {
            futures.add(executor.submit(() -> {
                readyLatch.countDown();
                startLatch.await();
                OrderCreatedEvent event = new OrderCreatedEvent(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        productId,
                        1,
                        new BigDecimal("50.00"),
                        Instant.now()
                );
                return coordinator.reserveWithRetry(event);
            }));
        }

        readyLatch.await();
        startLatch.countDown();

        long reservedCount = 0;
        long rejectedCount = 0;

        for (Future<Optional<InventoryResultEvent>> future : futures) {
            InventoryResultEvent result = future.get().orElseThrow();
            if (result.reserved()) {
                reservedCount++;
            } else {
                rejectedCount++;
            }
        }
        executor.shutdown();

        ProductInventory finalInventory = inventoryRepository.findById(productId).orElseThrow();
        assertThat(reservedCount).isEqualTo(3);
        assertThat(rejectedCount).isEqualTo(7);
        assertThat(finalInventory.getAvailableQuantity()).isZero();
    }
}
