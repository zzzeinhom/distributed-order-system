package com.zzzeinhom.order.consumer;

import com.zzzeinhom.order.dto.InventoryResultEvent;
import com.zzzeinhom.order.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventsConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventsConsumer.class);

    private final OrderService orderService;

    public InventoryEventsConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(
            topics = "inventory-events",
            groupId = "order-service-group",
            properties = {"spring.json.value.default.type=com.zzzeinhom.order.dto.InventoryResultEvent"}
    )
    public void onInventoryResult(InventoryResultEvent event) {
        log.info("Received InventoryResultEvent for order {} (reserved={})",
                event.orderId(), event.reserved());
        orderService.handleInventoryResult(event);
    }
}
