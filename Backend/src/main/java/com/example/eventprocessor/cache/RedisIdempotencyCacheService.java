package com.example.eventprocessor.cache;

import com.example.eventprocessor.config.IdempotencyProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Redis-backed implementation of IdempotencyCacheService.
 * Acts strictly as a read-through fast-path optimization for definitive COMPLETED events.
 * Fail-open design guarantees that Redis outages never compromise system correctness or fail events.
 */
@Service
public class RedisIdempotencyCacheService implements IdempotencyCacheService {

    private static final Logger log = LoggerFactory.getLogger(RedisIdempotencyCacheService.class);

    private static final String KEY_PREFIX = "event-processing:";
    private static final String COMPLETED_VALUE = "COMPLETED";

    private final StringRedisTemplate redisTemplate;
    private final IdempotencyProperties idempotencyProperties;

    public RedisIdempotencyCacheService(
            StringRedisTemplate redisTemplate,
            IdempotencyProperties idempotencyProperties) {
        this.redisTemplate = redisTemplate;
        this.idempotencyProperties = idempotencyProperties;
    }

    private String buildKey(String eventId) {
        return KEY_PREFIX + eventId;
    }

    @Override
    public boolean isCompleted(String eventId) {
        String key = buildKey(eventId);
        try {
            String value = redisTemplate.opsForValue().get(key);
            if (COMPLETED_VALUE.equals(value)) {
                log.info("Redis cache HIT: eventId='{}' is already COMPLETED", eventId);
                return true;
            }
            log.debug("Redis cache MISS: eventId='{}' not found in cache", eventId);
            return false;
        } catch (Exception e) {
            log.warn("Redis cache read failed for eventId='{}'. Falling back to PostgreSQL. Error: {}",
                    eventId, e.getMessage());
            return false; // Fail-open fallback to PostgreSQL
        }
    }

    @Override
    public void markCompleted(String eventId) {
        String key = buildKey(eventId);
        Duration ttl = idempotencyProperties.getCache().getTtl();
        try {
            redisTemplate.opsForValue().set(key, COMPLETED_VALUE, ttl);
            log.info("Redis cache populated: eventId='{}' marked as COMPLETED with TTL={}", eventId, ttl);
        } catch (Exception e) {
            log.warn("Redis cache write failed for eventId='{}'. State remains committed in PostgreSQL. Error: {}",
                    eventId, e.getMessage());
            // Do not rethrow; event is already committed in PostgreSQL
        }
    }

    @Override
    public void evict(String eventId) {
        String key = buildKey(eventId);
        try {
            redisTemplate.delete(key);
            log.debug("Redis cache evicted: eventId='{}'", eventId);
        } catch (Exception e) {
            log.warn("Redis cache eviction failed for eventId='{}'. Error: {}", eventId, e.getMessage());
        }
    }
}
