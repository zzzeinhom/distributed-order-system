package com.zzzeinhom.inventory.consumer;

import com.zzzeinhom.inventory.dto.OrderCreatedEvent;
import com.zzzeinhom.inventory.service.InventoryReservationCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class OrderEventsConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsConsumer.class);
    public static final String INVENTORY_EVENTS_TOPIC = "inventory-events";

    private final InventoryReservationCoordinator coordinator;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventsConsumer(InventoryReservationCoordinator coordinator,
                               KafkaTemplate<String, Object> kafkaTemplate) {
        this.coordinator = coordinator;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
            topics = "order-events",
            groupId = "inventory-service-group",
            properties = {"spring.json.value.default.type=com.zzzeinhom.inventory.dto.OrderCreatedEvent"}
    )
    public void onOrderCreated(OrderCreatedEvent event) throws Exception {
        log.info("Received OrderCreatedEvent for order {} (product={}, qty={})",
                event.orderId(), event.productId(), event.quantity());

        var resultOpt = coordinator.reserveWithRetry(event);

        if (resultOpt.isPresent()) {
            var result = resultOpt.get();
            kafkaTemplate.send(INVENTORY_EVENTS_TOPIC, result.orderId().toString(), result)
                    .get(5, TimeUnit.SECONDS);
            log.info("Published InventoryResultEvent (reserved={}) for order {}",
                    result.reserved(), result.orderId());
        }
    }
}
