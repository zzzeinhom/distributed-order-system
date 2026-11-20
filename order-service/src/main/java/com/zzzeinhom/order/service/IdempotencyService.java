package com.zzzeinhom.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;

@Service
public class IdempotencyService {

    private static final String KEY_PREFIX = "idempotency:order:";
    private static final String IN_PROGRESS = "IN_PROGRESS";
    private static final Duration LOCK_TTL = Duration.ofSeconds(30);
    private static final Duration RESPONSE_TTL = Duration.ofHours(24);

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public IdempotencyService(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    public LockResult tryAcquire(String idempotencyKey) {
        String redisKey = KEY_PREFIX + idempotencyKey;
        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(redisKey, IN_PROGRESS, LOCK_TTL);

        if (Boolean.TRUE.equals(acquired)) {
            return new LockResult(true, false, null);
        }

        String existingValue = redisTemplate.opsForValue().get(redisKey);
        if (existingValue == null || IN_PROGRESS.equals(existingValue)) {
            return new LockResult(false, true, null);
        }

        return new LockResult(false, false, existingValue);
    }

    public void saveResponse(String idempotencyKey, Object response) {
        try {
            String json = objectMapper.writeValueAsString(response);
            redisTemplate.opsForValue().set(KEY_PREFIX + idempotencyKey, json, RESPONSE_TTL);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize idempotency response", e);
        }
    }

    public void release(String idempotencyKey) {
        redisTemplate.delete(KEY_PREFIX + idempotencyKey);
    }

    public <T> Optional<T> deserialize(String json, Class<T> clazz) {
        if (json == null) return Optional.empty();
        try {
            return Optional.of(objectMapper.readValue(json, clazz));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize cached response", e);
        }
    }

    public record LockResult(boolean acquired, boolean inProgress, String cachedResponseJson) {}
}
