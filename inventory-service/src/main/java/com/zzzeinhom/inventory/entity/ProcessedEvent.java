package com.zzzeinhom.inventory.entity;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    @Column(name = "event_id")
    private UUID eventId;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "processed_at", nullable = false, updatable = false)
    private Instant processedAt;

    protected ProcessedEvent() {}

    public ProcessedEvent(UUID eventId, UUID orderId) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.processedAt = Instant.now();
    }

    public UUID getEventId() { return eventId; }
    public UUID getOrderId() { return orderId; }
}
