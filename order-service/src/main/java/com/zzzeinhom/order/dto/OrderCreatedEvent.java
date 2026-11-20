package com.zzzeinhom.order.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreatedEvent(
        UUID eventId,
        UUID orderId,
        UUID customerId,
        UUID productId,
        int quantity,
        BigDecimal totalAmount,
        Instant occurredAt
) {}
