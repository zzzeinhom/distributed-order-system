package com.zzzeinhom.order.controller;

import com.zzzeinhom.order.dto.CreateOrderRequest;
import com.zzzeinhom.order.dto.OrderResponse;
import com.zzzeinhom.order.service.IdempotencyService;
import com.zzzeinhom.order.service.OrderService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
@Validated
public class OrderController {

    private final OrderService orderService;
    private final IdempotencyService idempotencyService;

    public OrderController(OrderService orderService, IdempotencyService idempotencyService) {
        this.orderService = orderService;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(
            @RequestHeader("Idempotency-Key") @NotBlank String idempotencyKey,
            @Valid @RequestBody CreateOrderRequest request) {

        var lock = idempotencyService.tryAcquire(idempotencyKey);

        if (!lock.acquired()) {
            if (lock.inProgress()) {
                return ResponseEntity.status(HttpStatus.CONFLICT).build();
            }
            return idempotencyService.deserialize(lock.cachedResponseJson(), OrderResponse.class)
                    .map(ResponseEntity::ok)
                    .orElseGet(() -> ResponseEntity.status(HttpStatus.CONFLICT).build());
        }

        try {
            OrderResponse response = orderService.createOrder(idempotencyKey, request);
            idempotencyService.saveResponse(idempotencyKey, response);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
        } catch (DataIntegrityViolationException ex) {
            OrderResponse existing = orderService.getOrderByIdempotencyKey(idempotencyKey);
            idempotencyService.saveResponse(idempotencyKey, existing);
            return ResponseEntity.ok(existing);
        } catch (RuntimeException ex) {
            idempotencyService.release(idempotencyKey);
            throw ex;
        }
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderResponse> getOrder(@PathVariable UUID orderId) {
        return ResponseEntity.ok(orderService.getOrder(orderId));
    }
}
