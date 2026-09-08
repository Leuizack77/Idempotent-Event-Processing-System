package com.example.eventprocessor.idempotency;

/**
 * Indicates the outcome of an atomic reservation attempt for an event.
 */
public enum ReservationStatus {
    /**
     * The event was successfully reserved (or reclaimed) for processing by the calling thread.
     */
    RESERVED,

    /**
     * The event has already completed in the durable store. Terminal duplicate - safe to acknowledge.
     */
    ALREADY_COMPLETED,

    /**
     * The event is actively being processed by another worker within its lease window.
     * Transient non-terminal state - MUST NOT acknowledge.
     */
    ACTIVE_PROCESSING
}
