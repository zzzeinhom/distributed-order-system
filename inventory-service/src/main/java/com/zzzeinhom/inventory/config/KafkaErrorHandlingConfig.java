package com.zzzeinhom.inventory.config;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DeserializationException;

@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        // 1. Route failed records to <original-topic>.DLT while preserving the partition
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    log.error("Moving failed record (key={}, partition={}, offset={}) to DLT due to: {}",
                            record.key(), record.partition(), record.offset(), ex.getMessage());
                    return new TopicPartition(record.topic() + ".DLT", record.partition());
                }
        );

        // 2. Retry 3 times with exponential backoff (1s -> 2s -> 4s) for transient errors
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1_000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // 3. Immediately route non-retryable poison-pill exceptions to the DLT
        errorHandler.addNotRetryableExceptions(
                DeserializationException.class,
                IllegalArgumentException.class
        );

        return errorHandler;
    }
}
