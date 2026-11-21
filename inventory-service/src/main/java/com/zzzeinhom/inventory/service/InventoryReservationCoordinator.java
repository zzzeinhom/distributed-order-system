package com.zzzeinhom.inventory.service;

import com.zzzeinhom.inventory.dto.InventoryResultEvent;
import com.zzzeinhom.inventory.dto.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class InventoryReservationCoordinator {

    private static final Logger log = LoggerFactory.getLogger(InventoryReservationCoordinator.class);
    private static final int MAX_OPTIMISTIC_RETRIES = 15;

    private final InventoryService inventoryService;

    public InventoryReservationCoordinator(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    public Optional<InventoryResultEvent> reserveWithRetry(OrderCreatedEvent event) {
        int attempt = 0;
        while (true) {
            try {
                attempt++;
                return inventoryService.tryReserveStock(event);
            } catch (OptimisticLockingFailureException ex) {
                if (attempt >= MAX_OPTIMISTIC_RETRIES) {
                    log.error("Max optimistic lock retries reached for order {}", event.orderId(), ex);
                    throw ex;
                }
                log.warn("Optimistic lock conflict for order {} (attempt {}/{}), retrying...",
                        event.orderId(), attempt, MAX_OPTIMISTIC_RETRIES);
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(5, 25));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted during optimistic lock retry", ie);
                }
            }
        }
    }
}
