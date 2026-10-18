package com.example.eventprocessor.idempotency;

/**
 * Result of idempotency engine processing pipeline execution.
 */
public enum ProcessingResult {
    /**
     * Event was newly reserved, successfully executed, and marked COMPLETED.
     */
    SUCCESS,

    /**
     * Event was previously completed or is concurrently in-flight. Business effect was skipped.
     */
    SKIPPED_DUPLICATE,

    /**
     * Event processing failed.
     */
    FAILED
}
