package com.example.eventprocessor.exception;

/**
 * Thrown when a business entity (e.g. paymentId, orderId) already exists unexpectedly.
 */
public class DuplicateBusinessEntityException extends RuntimeException {

    public DuplicateBusinessEntityException(String message) {
        super(message);
    }

    public DuplicateBusinessEntityException(String message, Throwable cause) {
        super(message, cause);
    }
}