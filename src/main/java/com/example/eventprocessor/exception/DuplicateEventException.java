package com.example.eventprocessor.exception;

/**
 * Thrown when an incoming eventId already exists in the system.
 */
public class DuplicateEventException extends RuntimeException {

    public DuplicateEventException(String message) {
        super(message);
    }
}
