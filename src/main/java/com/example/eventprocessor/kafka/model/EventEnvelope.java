package com.example.eventprocessor.kafka.model;

import java.time.Instant;
import java.util.Map;

/**
 * Canonical Kafka event envelope transferred across topics.
 *
 * @param eventId   The unique event identifier (also used as Kafka message record key).
 * @param eventType The business event type classification.
 * @param timestamp The creation / submission timestamp.
 * @param payload   The domain-specific event payload.
 */
public record EventEnvelope(
        String eventId,
        String eventType,
        Instant timestamp,
        Map<String, Object> payload
) {
    public EventEnvelope {
        if (timestamp == null) {
            timestamp = Instant.now();
        }
    }

    public static EventEnvelope of(String eventId, String eventType, Map<String, Object> payload) {
        return new EventEnvelope(eventId, eventType, Instant.now(), payload);
    }
}
