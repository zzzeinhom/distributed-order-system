package com.zzzeinhom.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zzzeinhom.order.dto.CreateOrderRequest;
import com.zzzeinhom.order.dto.InventoryResultEvent;
import com.zzzeinhom.order.dto.OrderCreatedEvent;
import com.zzzeinhom.order.dto.OrderResponse;
import com.zzzeinhom.order.entity.Order;
import com.zzzeinhom.order.entity.OrderStatus;
import com.zzzeinhom.order.entity.OutboxEvent;
import com.zzzeinhom.order.repository.OrderRepository;
import com.zzzeinhom.order.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository,
                        OutboxEventRepository outboxRepository,
                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public OrderResponse createOrder(String idempotencyKey, CreateOrderRequest request) {
        var existingOrder = orderRepository.findByIdempotencyKey(idempotencyKey);
        if (existingOrder.isPresent()) {
            return OrderResponse.from(existingOrder.get());
        }

        UUID orderId = UUID.randomUUID();
        BigDecimal totalAmount = request.unitPrice().multiply(BigDecimal.valueOf(request.quantity()));

        Order order = new Order(
                orderId,
                idempotencyKey,
                request.customerId(),
                request.productId(),
                request.quantity(),
                totalAmount
        );
        orderRepository.save(order);

        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID(),
                orderId,
                request.customerId(),
                request.productId(),
                request.quantity(),
                totalAmount,
                Instant.now()
        );

        OutboxEvent outboxEvent = new OutboxEvent(
                "ORDER",
                orderId,
                "ORDER_CREATED",
                serializeEvent(event)
        );
        outboxRepository.save(outboxEvent);

        return OrderResponse.from(order);
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrderByIdempotencyKey(String idempotencyKey) {
        Order order = orderRepository.findByIdempotencyKey(idempotencyKey)
                .orElseThrow(() -> new NoSuchElementException("Order not found for idempotency key: " + idempotencyKey));
        return OrderResponse.from(order);
    }

    @Transactional
    public void handleInventoryResult(InventoryResultEvent event) {
        Order order = orderRepository.findById(event.orderId())
                .orElseThrow(() -> new NoSuchElementException("Order not found: " + event.orderId()));

        // Idempotency guard: safely ignore duplicate events if order status was already finalized
        if (order.getStatus() != OrderStatus.PENDING) {
            return;
        }

        if (event.reserved()) {
            order.markConfirmed();
        } else {
            order.markRejected(event.failureReason());
        }
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrder(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoSuchElementException("Order not found: " + orderId));
        return OrderResponse.from(order);
    }

    private String serializeEvent(OrderCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize OrderCreatedEvent", e);
        }
    }
}
