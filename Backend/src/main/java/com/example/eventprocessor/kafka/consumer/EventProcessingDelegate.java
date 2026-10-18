package com.example.eventprocessor.kafka.consumer;

import com.example.eventprocessor.kafka.model.EventEnvelope;

/**
 * Delegate interface for handling consumed events prior to Kafka manual offset acknowledgment.
 * In Phase 4, acts as a verifiable execution boundary.
 * In Phase 5, will be integrated with the durable idempotency engine.
 */
public interface EventProcessingDelegate {

    void process(EventEnvelope envelope);
}
