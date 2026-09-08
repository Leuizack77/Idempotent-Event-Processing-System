package com.example.eventprocessor.exception;

/**
 * Thrown when an event payload is missing required fields or has invalid values during business processing.
 */
public class InvalidEventPayloadException extends RuntimeException {

    public InvalidEventPayloadException(String message) {
        super(message);
    }

    public InvalidEventPayloadException(String message, Throwable cause) {
        super(message, cause);
    }
}