package com.example.eventprocessor.processing;

import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.exception.StaleProcessingLeaseException;
import com.example.eventprocessor.kafka.consumer.EventProcessingDelegate;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Coordinates business processing and processed_events status update
 * within a single atomic PostgreSQL transaction.
 */
@Service
public class TransactionalBusinessExecutor {

    private static final Logger log = LoggerFactory.getLogger(TransactionalBusinessExecutor.class);

    private final EventProcessorRegistry processorRegistry;
    private final ProcessedEventRepository processedEventRepository;
    private final EventProcessingDelegate eventProcessingDelegate;

    public TransactionalBusinessExecutor(
            EventProcessorRegistry processorRegistry,
            ProcessedEventRepository processedEventRepository,
            @Nullable EventProcessingDelegate eventProcessingDelegate) {
        this.processorRegistry = processorRegistry;
        this.processedEventRepository = processedEventRepository;
        this.eventProcessingDelegate = eventProcessingDelegate;
    }

    /**
     * Executes domain business logic and commits COMPLETED state atomically.
     * If business processing or fencing check fails, the transaction is completely rolled back.
     *
     * @param envelope        The event envelope.
     * @param processingToken The active fence token acquired during reservation.
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public void executeAndComplete(EventEnvelope envelope, String processingToken) {
        String eventId = envelope.eventId();
        log.info("Executing transactional business processing for eventId='{}', token='{}'", eventId, processingToken);

        // 1. If test hook delegate present, invoke it first
        if (eventProcessingDelegate != null) {
            eventProcessingDelegate.process(envelope);
        }

        // 2. Dispatch to domain business processor
        BusinessEventProcessor processor = processorRegistry.getRequiredProcessor(envelope.eventType());
        processor.process(envelope);

        // 3. Atomically transition processed_events to COMPLETED using the fencing token
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
            log.error("Fencing check failed inside transaction: eventId='{}' with token='{}' lost ownership before completion",
                    eventId, processingToken);
            throw new StaleProcessingLeaseException(eventId, processingToken);
        }

        log.info("Transactional business processing and status COMPLETED committed atomically for eventId='{}'", eventId);
    }
}