package com.zzzeinhom.order.dto;

import java.time.Instant;
import java.util.UUID;

public record InventoryResultEvent(
        UUID eventId,
        UUID orderId,
        UUID productId,
        boolean reserved,
        String failureReason,
        Instant occurredAt
) {}
