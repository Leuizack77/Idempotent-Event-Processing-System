package com.example.eventprocessor.idempotency;

import com.example.eventprocessor.entity.ProcessedEventEntity;
import com.example.eventprocessor.kafka.model.EventEnvelope;

import java.util.Optional;

/**
 * Service responsible for atomic, durable state transitions in PostgreSQL.
 * Manages reservation, completion, and failure within safe transaction boundaries.
 */
public interface IdempotencyService {

    /**
     * Atomically reserves an event for processing using PostgreSQL as the authoritative store.
     * Prevents race conditions and check-then-act vulnerabilities.
     *
     * @param envelope the event envelope
     * @return the reservation result indicating whether the event was reserved or already completed/processing
     */
    ReservationResult tryReserve(EventEnvelope envelope);

    /**
     * Transitions an event from PROCESSING to COMPLETED atomically with processed_at timestamp,
     * validating that the caller holds the active processingToken.
     *
     * @param eventId         the unique event identifier
     * @param processingToken the ownership token issued during tryReserve
     * @return true if the event was in PROCESSING state with matching token and transitioned to COMPLETED; false otherwise
     */
    boolean markCompleted(String eventId, String processingToken);

    /**
     * Transitions an event from PROCESSING to FAILED with the failure reason,
     * validating that the caller holds the active processingToken.
     *
     * @param eventId         the unique event identifier
     * @param processingToken the ownership token issued during tryReserve
     * @param errorMessage    description of the failure
     * @return true if the event was in PROCESSING state with matching token and transitioned to FAILED; false otherwise
     */
    boolean markFailed(String eventId, String processingToken, String errorMessage);

    /**
     * Prepares an event for a subsequent retry attempt after a transient failure,
     * retaining PROCESSING status while expiring the lease so that the next retry
     * can atomically re-reserve the event.
     *
     * @param eventId         the unique event identifier
     * @param processingToken the ownership token issued during tryReserve
     * @param errorMessage    description of the transient failure
     * @return true if the event was in PROCESSING state with matching token and was prepared; false otherwise
     */
    boolean prepareForRetry(String eventId, String processingToken, String errorMessage);

    /**
     * Retrieves the durable event record if present.
     *
     * @param eventId the unique event identifier
     * @return optional containing the entity if found
     */
    Optional<ProcessedEventEntity> findEvent(String eventId);
}
