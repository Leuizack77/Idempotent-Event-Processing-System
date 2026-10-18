package com.example.eventprocessor.idempotency;

import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.entity.ProcessedEventEntity;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * PostgreSQL-backed implementation of IdempotencyService.
 * Executes reservation and status transitions within isolated transactions (REQUIRES_NEW)
 * to guarantee that concurrent collisions do not poison calling transactions.
 */
@Service
public class IdempotencyServiceImpl implements IdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyServiceImpl.class);

    private final ProcessedEventRepository processedEventRepository;
    private final com.example.eventprocessor.config.IdempotencyProperties idempotencyProperties;

    public IdempotencyServiceImpl(
            ProcessedEventRepository processedEventRepository,
            com.example.eventprocessor.config.IdempotencyProperties idempotencyProperties) {
        this.processedEventRepository = processedEventRepository;
        this.idempotencyProperties = idempotencyProperties;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ReservationResult tryReserve(EventEnvelope envelope) {
        String eventId = envelope.eventId();
        String eventType = envelope.eventType();
        Instant now = Instant.now();
        Instant staleThreshold = now.minus(idempotencyProperties.getProcessingLeaseDuration());
        String candidateToken = UUID.randomUUID().toString();

        // 1. Check if the event already exists in PostgreSQL
        Optional<ProcessedEventEntity> existingOpt = processedEventRepository.findByEventId(eventId);
        if (existingOpt.isPresent()) {
            ProcessedEventEntity existing = existingOpt.get();
            if (existing.getStatus() == EventStatus.COMPLETED) {
                log.info("Event eventId='{}' already COMPLETED. Terminal duplicate detected, skipping execution.", eventId);
                return ReservationResult.alreadyCompleted(eventId);
            }

            // Attempt atomic state transition or stale lease reclaim with candidateToken
            int updated = processedEventRepository.transitionToProcessing(eventId, candidateToken, staleThreshold, now);
            if (updated > 0) {
                log.info("Successfully reserved/reclaimed eventId='{}' from status={} to PROCESSING with token='{}'",
                        eventId, existing.getStatus(), candidateToken);
                return ReservationResult.reserved(eventId, candidateToken);
            }

            // Another concurrent transaction updated the status first or event is actively processing within lease
            ProcessedEventEntity current = processedEventRepository.findByEventId(eventId).orElse(existing);
            if (current.getStatus() == EventStatus.COMPLETED) {
                return ReservationResult.alreadyCompleted(eventId);
            }

            log.warn("Event eventId='{}' is actively PROCESSING within lease window (currentToken='{}'). Rejecting duplicate attempt.",
                    eventId, current.getProcessingToken());
            return ReservationResult.activeProcessing(eventId);
        }

        // 2. If row does not exist yet (e.g. direct Kafka delivery not pre-registered via REST)
        // Perform atomic insert with ON CONFLICT DO NOTHING to avoid check-then-act race
        int inserted = processedEventRepository.insertIfNotExists(
                eventId,
                eventType,
                EventStatus.PROCESSING.name(),
                candidateToken,
                now
        );
        if (inserted > 0) {
            log.info("Successfully inserted and reserved new eventId='{}' in PROCESSING status with token='{}'",
                    eventId, candidateToken);
            return ReservationResult.reserved(eventId, candidateToken);
        }

        // Another concurrent thread won the insert race. Inspect its committed state.
        ProcessedEventEntity current = processedEventRepository.findByEventId(eventId).orElse(null);
        if (current != null) {
            if (current.getStatus() == EventStatus.COMPLETED) {
                log.info("Event eventId='{}' was already completed by another transaction. Skipping.", eventId);
                return ReservationResult.alreadyCompleted(eventId);
            }
            if (current.getStatus() == EventStatus.SUBMITTED ||
                    current.getStatus() == EventStatus.RECEIVED ||
                    current.getStatus() == EventStatus.FAILED) {
                int updated = processedEventRepository.transitionToProcessing(eventId, candidateToken, staleThreshold, now);
                if (updated > 0) {
                    log.info("Successfully reserved eventId='{}' from status={} to PROCESSING with token='{}'",
                            eventId, current.getStatus(), candidateToken);
                    return ReservationResult.reserved(eventId, candidateToken);
                }
            }
        }

        log.warn("Event eventId='{}' was reserved by another concurrent transaction and is actively PROCESSING.", eventId);
        return ReservationResult.activeProcessing(eventId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean markCompleted(String eventId, String processingToken) {
        Instant now = Instant.now();
        int updated = processedEventRepository.updateStatus(
                eventId,
                EventStatus.COMPLETED,
                EventStatus.PROCESSING,
                processingToken,
                now,
                null,
                now
        );
        if (updated == 0) {
            log.warn("markCompleted: eventId='{}' was not in PROCESSING state with token='{}'", eventId, processingToken);
            return false;
        }
        log.info("Successfully marked eventId='{}' as COMPLETED with token='{}'", eventId, processingToken);
        return true;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean markFailed(String eventId, String processingToken, String errorMessage) {
        Instant now = Instant.now();
        int updated = processedEventRepository.updateStatus(
                eventId,
                EventStatus.FAILED,
                EventStatus.PROCESSING,
                processingToken,
                null,
                errorMessage,
                now
        );
        if (updated == 0) {
            log.warn("markFailed: eventId='{}' was not in PROCESSING state with token='{}'", eventId, processingToken);
            return false;
        }
        log.info("Successfully marked eventId='{}' as FAILED with token='{}': {}", eventId, processingToken, errorMessage);
        return true;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean prepareForRetry(String eventId, String processingToken, String errorMessage) {
        Instant now = Instant.now();
        Instant expiredTimestamp = now.minus(idempotencyProperties.getProcessingLeaseDuration()).minusSeconds(1);
        int updated = processedEventRepository.prepareForRetry(
                eventId,
                processingToken,
                expiredTimestamp,
                errorMessage
        );
        if (updated == 0) {
            log.warn("prepareForRetry: eventId='{}' was not in PROCESSING state with token='{}'", eventId, processingToken);
            return false;
        }
        log.info("Successfully prepared eventId='{}' for retry with token='{}': {}", eventId, processingToken, errorMessage);
        return true;
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<ProcessedEventEntity> findEvent(String eventId) {
        return processedEventRepository.findByEventId(eventId);
    }
}
