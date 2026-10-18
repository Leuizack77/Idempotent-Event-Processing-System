package com.example.eventprocessor.exception;

/**
 * Thrown when a worker attempts to complete or update an event whose processing lease
 * has expired and been reclaimed by another worker (fencing token mismatch).
 */
public class StaleProcessingLeaseException extends RuntimeException {

    public StaleProcessingLeaseException(String eventId, String processingToken) {
        super("Processing lease expired or reclaimed for eventId '" + eventId + "' with token '" + processingToken + "'");
    }
}
