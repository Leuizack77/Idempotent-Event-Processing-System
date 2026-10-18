package com.example.eventprocessor.entity;

/**
 * Durable event processing status stored in PostgreSQL.
 *
 * State transitions:
 * SUBMITTED   -> The REST request was validated and published to Kafka.
 * RECEIVED    -> The Kafka consumer received the event.
 * PROCESSING  -> The event is actively being processed in the business workflow.
 * COMPLETED   -> The business operation and event state were successfully committed.
 * FAILED      -> The event reached a terminal failure state and was routed to the DLQ.
 */
public enum EventStatus {
    SUBMITTED,
    RECEIVED,
    PROCESSING,
    COMPLETED,
    FAILED
}
