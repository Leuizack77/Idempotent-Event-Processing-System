package com.example.eventprocessor.idempotency;

import com.example.eventprocessor.kafka.model.EventEnvelope;

/**
 * Idempotency engine orchestrating reservation, business execution, and state completion.
 */
public interface IdempotencyEngine {

    /**
     * Processes an event envelope idempotently. Guarantees exactly-once execution of the business
     * delegate for any given eventId across sequential and concurrent deliveries.
     *
     * @param envelope the event envelope
     * @return the processing result (SUCCESS, SKIPPED_DUPLICATE, or FAILED)
     */
    ProcessingResult process(EventEnvelope envelope);
}
