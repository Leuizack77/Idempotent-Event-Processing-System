package com.example.eventprocessor.exception;

/**
 * Thrown when an event request contains unsupported or invalid attributes.
 */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message) {
        super(message);
    }
}
