package com.zzzeinhom.order.dto;

import com.zzzeinhom.order.entity.Order;
import com.zzzeinhom.order.entity.OrderStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
        UUID orderId,
        UUID customerId,
        UUID productId,
        int quantity,
        BigDecimal totalAmount,
        OrderStatus status,
        String failureReason,
        Instant createdAt
) {
    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getProductId(),
                order.getQuantity(),
                order.getTotalAmount(),
                order.getStatus(),
                order.getFailureReason(),
                order.getCreatedAt()
        );
    }
}
