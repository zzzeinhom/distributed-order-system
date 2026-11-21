package com.zzzeinhom.inventory.entity;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "inventory")
public class ProductInventory {

    @Id
    @Column(name = "product_id")
    private UUID productId;

    @Column(nullable = false, unique = true, length = 50)
    private String sku;

    @Column(name = "available_quantity", nullable = false)
    private int availableQuantity;

    @Version
    @Column(nullable = false)
    private Long version;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected ProductInventory() {}

    public boolean hasEnoughStock(int requestedQuantity) {
        return this.availableQuantity >= requestedQuantity;
    }

    public void reserve(int quantity) {
        if (!hasEnoughStock(quantity)) {
            throw new IllegalStateException("Insufficient stock for product: " + productId);
        }
        this.availableQuantity -= quantity;
        this.updatedAt = Instant.now();
    }

    public UUID getProductId() { return productId; }
    public String getSku() { return sku; }
    public int getAvailableQuantity() { return availableQuantity; }
    public Long getVersion() { return version; }
}
