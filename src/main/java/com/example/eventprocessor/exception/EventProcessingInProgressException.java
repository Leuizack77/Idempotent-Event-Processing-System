package com.example.eventprocessor.exception;

/**
 * Thrown when an event is currently being actively processed by another consumer within its lease window.
 * This represents a transient/in-progress condition where the event must NOT be acknowledged
 * and must remain eligible for Kafka redelivery.
 */
public class EventProcessingInProgressException extends RuntimeException {

    public EventProcessingInProgressException(String eventId) {
        super("Event with eventId '" + eventId + "' is actively in PROCESSING state within lease window");
    }
}
