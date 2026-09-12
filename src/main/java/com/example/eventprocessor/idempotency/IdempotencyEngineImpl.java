package com.example.eventprocessor.idempotency;

import com.example.eventprocessor.cache.IdempotencyCacheService;
import com.example.eventprocessor.exception.EventProcessingInProgressException;
import com.example.eventprocessor.exception.StaleProcessingLeaseException;
import com.example.eventprocessor.kafka.consumer.KafkaRetryClassifier;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.metrics.EventMetrics;
import com.example.eventprocessor.processing.TransactionalBusinessExecutor;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/**
 * Default implementation of IdempotencyEngine.
 * Orchestrates Redis fast-path caching, PostgreSQL event reservation,
 * transactional business execution with atomic completion, and isolated failure / retry transitions.
 * Tracks Micrometer metrics for processed events, duplicates, and processing durations.
 */
@Component
public class IdempotencyEngineImpl implements IdempotencyEngine {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyEngineImpl.class);

    private final IdempotencyService idempotencyService;
    private final TransactionalBusinessExecutor transactionalBusinessExecutor;
    private final KafkaRetryClassifier retryClassifier;
    private final IdempotencyCacheService idempotencyCacheService;
    private final EventMetrics eventMetrics;

    @Autowired
    public IdempotencyEngineImpl(
            IdempotencyService idempotencyService,
            TransactionalBusinessExecutor transactionalBusinessExecutor,
            KafkaRetryClassifier retryClassifier,
            IdempotencyCacheService idempotencyCacheService,
            @Nullable EventMetrics eventMetrics) {
        this.idempotencyService = idempotencyService;
        this.transactionalBusinessExecutor = transactionalBusinessExecutor;
        this.retryClassifier = retryClassifier;
        this.idempotencyCacheService = idempotencyCacheService;
        this.eventMetrics = eventMetrics;
    }

    public IdempotencyEngineImpl(
            IdempotencyService idempotencyService,
            TransactionalBusinessExecutor transactionalBusinessExecutor,
            KafkaRetryClassifier retryClassifier,
            IdempotencyCacheService idempotencyCacheService) {
        this(idempotencyService, transactionalBusinessExecutor, retryClassifier, idempotencyCacheService, null);
    }

    @Override
    public ProcessingResult process(EventEnvelope envelope) {
        String eventId = envelope.eventId();
        String eventType = envelope.eventType();
        log.info("Idempotency engine evaluating event: eventId={}, eventType={}", eventId, eventType);

        Timer.Sample timerSample = eventMetrics != null ? eventMetrics.startTimer() : null;
        boolean timerStopped = false;

        try {
            // 1. Redis fast-path read (bypasses PostgreSQL reservation on cache hit)
            if (idempotencyCacheService.isCompleted(eventId)) {
                log.info("Redis cache HIT: eventId={} is already COMPLETED. Skipping PostgreSQL reservation and business execution.", eventId);
                if (eventMetrics != null) {
                    eventMetrics.incrementDuplicate(eventType, "REDIS");
                    eventMetrics.stopTimer(timerSample, eventType, "DUPLICATE");
                    timerStopped = true;
                }
                return ProcessingResult.SKIPPED_DUPLICATE;
            }

            // 2. PostgreSQL authoritative reservation (isolated transaction)
            ReservationResult reservation = idempotencyService.tryReserve(envelope);

            if (reservation.isAlreadyCompleted()) {
                log.info("Duplicate event detected in PostgreSQL (already COMPLETED): eventId={}. Populating cache and skipping business execution.", eventId);
                idempotencyCacheService.markCompleted(eventId);
                if (eventMetrics != null) {
                    eventMetrics.incrementDuplicate(eventType, "POSTGRES");
                    eventMetrics.stopTimer(timerSample, eventType, "DUPLICATE");
                    timerStopped = true;
                }
                return ProcessingResult.SKIPPED_DUPLICATE;
            }

            if (reservation.isActiveProcessing()) {
                log.warn("Event eventId={} is actively PROCESSING within lease window. Rejecting duplicate attempt to allow redelivery.", eventId);
                if (eventMetrics != null) {
                    eventMetrics.stopTimer(timerSample, eventType, "FAILED");
                    timerStopped = true;
                }
                throw new EventProcessingInProgressException(eventId);
            }

            // 3. Reservation acquired: execute business logic & commit COMPLETED atomically
            String token = reservation.processingToken();
            log.info("Event successfully reserved: eventId={}, token={}. Executing business transaction.", eventId, token);
            try {
                transactionalBusinessExecutor.executeAndComplete(envelope, token);
                log.info("Business execution and COMPLETED state successfully committed: eventId={}, token={}", eventId, token);

                // 4. Post-commit non-transactional cache write
                idempotencyCacheService.markCompleted(eventId);
                if (eventMetrics != null) {
                    eventMetrics.incrementProcessed(eventType);
                    eventMetrics.stopTimer(timerSample, eventType, "SUCCESS");
                    timerStopped = true;
                }
                return ProcessingResult.SUCCESS;
            } catch (StaleProcessingLeaseException e) {
                log.warn("Worker fenced out for eventId={}: {}. A newer worker has taken over processing. Dropping execution without modifying state.",
                        eventId, e.getMessage());
                if (eventMetrics != null && !timerStopped) {
                    eventMetrics.stopTimer(timerSample, eventType, "FAILED");
                    timerStopped = true;
                }
                throw e;
            } catch (Exception e) {
                if (eventMetrics != null && !timerStopped) {
                    eventMetrics.stopTimer(timerSample, eventType, "FAILED");
                    timerStopped = true;
                }
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
        } finally {
            if (timerSample != null && !timerStopped) {
                eventMetrics.stopTimer(timerSample, eventType, "FAILED");
            }
        }
    }
}
