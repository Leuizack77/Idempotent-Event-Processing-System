package com.example.eventprocessor.idempotency;

import com.example.eventprocessor.exception.EventProcessingInProgressException;
import com.example.eventprocessor.exception.StaleProcessingLeaseException;
import com.example.eventprocessor.kafka.consumer.KafkaRetryClassifier;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.processing.TransactionalBusinessExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Default implementation of IdempotencyEngine.
 * Orchestrates event reservation, transactional business execution with atomic completion,
 * and isolated failure / retry transitions.
 */
@Component
public class IdempotencyEngineImpl implements IdempotencyEngine {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyEngineImpl.class);

    private final IdempotencyService idempotencyService;
    private final TransactionalBusinessExecutor transactionalBusinessExecutor;
    private final KafkaRetryClassifier retryClassifier;

    public IdempotencyEngineImpl(
            IdempotencyService idempotencyService,
            TransactionalBusinessExecutor transactionalBusinessExecutor,
            KafkaRetryClassifier retryClassifier) {
        this.idempotencyService = idempotencyService;
        this.transactionalBusinessExecutor = transactionalBusinessExecutor;
        this.retryClassifier = retryClassifier;
    }

    @Override
    public ProcessingResult process(EventEnvelope envelope) {
        String eventId = envelope.eventId();
        log.info("Idempotency engine evaluating event: eventId={}, eventType={}", eventId, envelope.eventType());

        // 1. PostgreSQL authoritative reservation (isolated transaction)
        ReservationResult reservation = idempotencyService.tryReserve(envelope);

        if (reservation.isAlreadyCompleted()) {
            log.info("Duplicate event detected in PostgreSQL (already COMPLETED): eventId={}. Skipping business execution.", eventId);
            return ProcessingResult.SKIPPED_DUPLICATE;
        }

        if (reservation.isActiveProcessing()) {
            log.warn("Event eventId={} is actively PROCESSING within lease window. Rejecting duplicate attempt to allow redelivery.", eventId);
            throw new EventProcessingInProgressException(eventId);
        }

        // 2. Reservation acquired: execute business logic & commit COMPLETED atomically
        String token = reservation.processingToken();
        log.info("Event successfully reserved: eventId={}, token={}. Executing business transaction.", eventId, token);
        try {
            transactionalBusinessExecutor.executeAndComplete(envelope, token);
            log.info("Business execution and COMPLETED state successfully committed: eventId={}, token={}", eventId, token);
            return ProcessingResult.SUCCESS;
        } catch (StaleProcessingLeaseException e) {
            log.warn("Worker fenced out for eventId={}: {}. A newer worker has taken over processing. Dropping execution without modifying state.",
                    eventId, e.getMessage());
            throw e;
        } catch (Exception e) {
            if (retryClassifier.isRetryable(e)) {
                log.warn("Transient failure processing eventId={}: {}. Preparing for retry while keeping PROCESSING state.",
                        eventId, e.getMessage());
                idempotencyService.prepareForRetry(eventId, token, e.getMessage());
            } else {
                log.error("Fatal/terminal failure processing eventId={}: {}. Marking state as FAILED with token={}.",
                        eventId, e.getMessage(), token, e);
                idempotencyService.markFailed(eventId, token, e.getMessage());
            }
            throw e; // Rethrow to let listener container / DefaultErrorHandler manage retry or DLQ
        }
    }
}
