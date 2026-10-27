package com.example.eventprocessor.service;

import com.example.eventprocessor.config.MdcFilter;
import com.example.eventprocessor.dto.request.EventRequestDto;
import com.example.eventprocessor.dto.response.EventResponseDto;
import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.entity.ProcessedEventEntity;
import com.example.eventprocessor.exception.DuplicateEventException;
import com.example.eventprocessor.exception.InvalidEventException;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.kafka.producer.KafkaEventProducer;
import com.example.eventprocessor.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

@Service
public class EventIngestionServiceImpl implements EventIngestionService {

    private static final Logger log = LoggerFactory.getLogger(EventIngestionServiceImpl.class);

    private static final Set<String> SUPPORTED_EVENT_TYPES = Set.of(
            "PAYMENT_COMPLETED",
            "PAYMENT_PROCESSED",
            "ORDER_CREATED"
    );

    private final ProcessedEventRepository processedEventRepository;
    private final KafkaEventProducer kafkaEventProducer;

    public EventIngestionServiceImpl(
            ProcessedEventRepository processedEventRepository,
            KafkaEventProducer kafkaEventProducer) {
        this.processedEventRepository = processedEventRepository;
        this.kafkaEventProducer = kafkaEventProducer;
    }

    @Override
    @Transactional
    public EventResponseDto ingestEvent(EventRequestDto request) {
        String eventId = request.eventId().trim();
        String eventType = request.eventType().trim().toUpperCase();

        MDC.put(MdcFilter.EVENT_ID_KEY, eventId);
        MDC.put(MdcFilter.EVENT_TYPE_KEY, eventType);

        validateEventType(eventType);
        validatePayload(eventType, request.payload());

        if (processedEventRepository.existsByEventId(eventId)) {
            log.warn("Event submission rejected: eventId '{}' already exists", eventId);
            throw new DuplicateEventException("Event with eventId '" + eventId + "' already exists");
        }

        ProcessedEventEntity entity = new ProcessedEventEntity(eventId, eventType, EventStatus.SUBMITTED);
        try {
            processedEventRepository.save(entity);
        } catch (DataIntegrityViolationException e) {
            log.warn("Database unique constraint violation on ingestion for eventId: {}", eventId);
            throw new DuplicateEventException("Event with eventId '" + eventId + "' already exists");
        }

        log.info("Event successfully accepted and persisted as SUBMITTED: eventId={}, eventType={}", eventId, eventType);

        // Publish to Kafka with eventId as record key
        EventEnvelope envelope = EventEnvelope.of(eventId, eventType, request.payload());
        kafkaEventProducer.sendEvent(envelope);

        Instant submittedAt = entity.getCreatedAt() != null ? entity.getCreatedAt() : Instant.now();

        return new EventResponseDto(
                eventId,
                EventStatus.SUBMITTED.name(),
                "Event accepted for asynchronous processing",
                submittedAt
        );
    }

    private void validateEventType(String eventType) {
        if (!SUPPORTED_EVENT_TYPES.contains(eventType)) {
            log.warn("Unsupported eventType rejected: {}", eventType);
            throw new InvalidEventException("Unsupported eventType: '" + eventType + "'. Supported types: " + SUPPORTED_EVENT_TYPES);
        }
    }

    private void validatePayload(String eventType, Map<String, Object> payload) {
        if (payload == null || payload.isEmpty()) {
            throw new InvalidEventException("payload must not be empty");
        }

        if ("PAYMENT_COMPLETED".equals(eventType) || "PAYMENT_PROCESSED".equals(eventType)) {
            requireField(payload, "paymentId");
            requireField(payload, "userId");
            requireField(payload, "amount");
            requireField(payload, "currency");
        } else if ("ORDER_CREATED".equals(eventType)) {
            requireField(payload, "orderId");
            requireField(payload, "customerId");
            requireField(payload, "totalAmount");
            requireField(payload, "itemCount");
        }
    }

    private void requireField(Map<String, Object> payload, String fieldName) {
        if (!payload.containsKey(fieldName) || payload.get(fieldName) == null || payload.get(fieldName).toString().isBlank()) {
            throw new InvalidEventException("Missing or blank required field in payload: '" + fieldName + "'");
        }
    }
}
