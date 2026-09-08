package com.example.eventprocessor.processing;

import com.example.eventprocessor.kafka.model.EventEnvelope;

/**
 * Strategy interface for domain-specific business event processing.
 */
public interface BusinessEventProcessor {

    /**
     * @return The eventType this processor handles (e.g. "PAYMENT_COMPLETED", "ORDER_CREATED").
     */
    String getSupportedEventType();

    /**
     * Executes domain business mutation for the given event envelope.
     * Must be invoked within an active Spring transaction.
     *
     * @param envelope The incoming event envelope containing payload.
     */
    void process(EventEnvelope envelope);
}