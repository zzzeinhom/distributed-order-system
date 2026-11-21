package com.zzzeinhom.inventory.dto;

import java.time.Instant;
import java.util.UUID;

public record InventoryResultEvent(
        UUID eventId,
        UUID orderId,
        UUID productId,
        boolean reserved,
        String failureReason,
        Instant occurredAt
) {
    public static InventoryResultEvent success(UUID orderId, UUID productId) {
        return new InventoryResultEvent(UUID.randomUUID(), orderId, productId, true, null, Instant.now());
    }

    public static InventoryResultEvent rejected(UUID orderId, UUID productId, String reason) {
        return new InventoryResultEvent(UUID.randomUUID(), orderId, productId, false, reason, Instant.now());
    }
}
