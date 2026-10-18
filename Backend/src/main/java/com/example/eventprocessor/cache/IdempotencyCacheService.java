package com.example.eventprocessor.cache;

/**
 * Cache abstraction layer decoupling the idempotency engine from Redis.
 * Only stores definitive COMPLETED state.
 */
public interface IdempotencyCacheService {

    /**
     * Checks if the event has already definitively completed.
     *
     * @param eventId The unique event ID.
     * @return true if the event is cached as COMPLETED, false otherwise or if the cache is unavailable.
     */
    boolean isCompleted(String eventId);

    /**
     * Records the definitive COMPLETED state for the given event ID.
     *
     * @param eventId The unique event ID.
     */
    void markCompleted(String eventId);

    /**
     * Evicts the cached completion record for the given event ID.
     *
     * @param eventId The unique event ID.
     */
    void evict(String eventId);
}
