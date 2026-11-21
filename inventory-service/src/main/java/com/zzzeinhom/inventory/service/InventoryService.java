package com.zzzeinhom.inventory.service;

import com.zzzeinhom.inventory.dto.InventoryResultEvent;
import com.zzzeinhom.inventory.dto.OrderCreatedEvent;
import com.zzzeinhom.inventory.entity.ProcessedEvent;
import com.zzzeinhom.inventory.entity.ProductInventory;
import com.zzzeinhom.inventory.repository.ProcessedEventRepository;
import com.zzzeinhom.inventory.repository.ProductInventoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final ProductInventoryRepository inventoryRepository;
    private final ProcessedEventRepository processedEventRepository;

    public InventoryService(ProductInventoryRepository inventoryRepository,
                            ProcessedEventRepository processedEventRepository) {
        this.inventoryRepository = inventoryRepository;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public Optional<InventoryResultEvent> tryReserveStock(OrderCreatedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            log.warn("Duplicate event {} detected for order {}, skipping.", event.eventId(), event.orderId());
            return Optional.empty();
        }

        processedEventRepository.save(new ProcessedEvent(event.eventId(), event.orderId()));

        ProductInventory inventory = inventoryRepository.findById(event.productId()).orElse(null);
        if (inventory == null) {
            return Optional.of(InventoryResultEvent.rejected(
                    event.orderId(), event.productId(), "Product not found in inventory"));
        }

        if (!inventory.hasEnoughStock(event.quantity())) {
            log.info("Insufficient stock for order {} (requested={}, available={})",
                    event.orderId(), event.quantity(), inventory.getAvailableQuantity());
            return Optional.of(InventoryResultEvent.rejected(
                    event.orderId(), event.productId(), "Insufficient stock available"));
        }

        inventory.reserve(event.quantity());
        inventoryRepository.saveAndFlush(inventory);

        log.info("Reserved {} units of product {} for order {} (remaining={})",
                event.quantity(), event.productId(), event.orderId(), inventory.getAvailableQuantity());

        return Optional.of(InventoryResultEvent.success(event.orderId(), event.productId()));
    }
}
