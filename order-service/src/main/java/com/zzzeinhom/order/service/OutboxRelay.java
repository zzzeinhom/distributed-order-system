package com.zzzeinhom.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zzzeinhom.order.dto.OrderCreatedEvent;
import com.zzzeinhom.order.entity.OutboxEvent;
import com.zzzeinhom.order.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    public static final String ORDER_EVENTS_TOPIC = "order-events";
    private static final int BATCH_SIZE = 50;
    private static final int MAX_RETRIES = 5;

    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OutboxRelay(OutboxEventRepository outboxRepository,
                       KafkaTemplate<String, Object> kafkaTemplate,
                       ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> batch = outboxRepository.findBatchForProcessing(BATCH_SIZE);
        for (OutboxEvent outbox : batch) {
            try {
                OrderCreatedEvent event = objectMapper.readValue(outbox.getPayload(), OrderCreatedEvent.class);
                // Partition Key = productId to guarantee strict ordering per product
                kafkaTemplate.send(ORDER_EVENTS_TOPIC, event.productId().toString(), event)
                        .get(5, TimeUnit.SECONDS);

                outbox.markProcessed();
                log.info("Published outbox event {} for order {}", outbox.getId(), outbox.getAggregateId());
            } catch (Exception ex) {
                outbox.incrementRetryOrFail(MAX_RETRIES);
                log.error("Failed to publish outbox event {} (attempt {})", outbox.getId(), outbox.getRetryCount(), ex);
            }
        }
    }
}
