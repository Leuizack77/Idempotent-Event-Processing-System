package com.example.eventprocessor.kafka.consumer;

import com.example.eventprocessor.config.KafkaConfig;
import com.example.eventprocessor.config.MdcFilter;
import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * Dedicated consumer listening to the Dead Letter Queue (events.dlq).
 * Captures terminal event failures, records diagnostics, updates PostgreSQL status to FAILED,
 * and preserves poison pills for audit and administrative inspection.
 * Populates MDC context for structured DLQ logging.
 */
@Component
public class KafkaDlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaDlqConsumer.class);

    private final ProcessedEventRepository processedEventRepository;

    public KafkaDlqConsumer(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    @KafkaListener(
            topics = KafkaConfig.TOPIC_EVENTS_DLQ,
            groupId = "${spring.kafka.consumer.dlq-group-id:event-processor-dlq-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDlq(
            org.apache.kafka.clients.consumer.ConsumerRecord<String, EventEnvelope> record,
            @Header(value = "X-Exception-Message", required = false) String exceptionMessage,
            @Header(value = "X-Retry-Count", required = false) String retryCount,
            Acknowledgment acknowledgment
    ) {
        EventEnvelope envelope = record.value();
        String key = record.key();
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();

        String traceId = null;
        org.apache.kafka.common.header.Header traceHeader = record.headers().lastHeader(MdcFilter.HEADER_TRACE_ID);
        if (traceHeader != null && traceHeader.value() != null) {
            traceId = new String(traceHeader.value(), StandardCharsets.UTF_8).trim();
        }
        if (traceId == null || traceId.isBlank()) {
            org.apache.kafka.common.header.Header corrHeader = record.headers().lastHeader(MdcFilter.HEADER_CORRELATION_ID);
            if (corrHeader != null && corrHeader.value() != null) {
                traceId = new String(corrHeader.value(), StandardCharsets.UTF_8).trim();
            }
        }
        if (traceId == null || traceId.isBlank()) {
            traceId = "tr-" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        }

        String eventId = envelope != null ? envelope.eventId() : key;
        String eventType = envelope != null ? envelope.eventType() : "UNKNOWN";

        MDC.put(MdcFilter.TRACE_ID_KEY, traceId);
        MDC.put(MdcFilter.CORRELATION_ID_KEY, traceId);
        MDC.put(MdcFilter.EVENT_ID_KEY, eventId != null ? eventId : "");
        MDC.put(MdcFilter.EVENT_TYPE_KEY, eventType);

        try {
            log.warn("DLQ Record received: topic={}, partition={}, offset={}, key={}, eventId={}, retryCount={}, error={}",
                    topic, partition, offset, key, eventId, retryCount, exceptionMessage);

            if (eventId != null) {
                try {
                    processedEventRepository.findByEventId(eventId).ifPresent(entity -> {
                        if (entity.getStatus() != EventStatus.COMPLETED) {
                            entity.setStatus(EventStatus.FAILED);
                            if (exceptionMessage != null) {
                                entity.setErrorMessage(exceptionMessage);
                            }
                            entity.setUpdatedAt(Instant.now());
                            processedEventRepository.save(entity);
                            log.info("Persisted DLQ terminal status FAILED for eventId='{}'", eventId);
                        }
                    });
                } catch (Exception e) {
                    log.error("Failed to update status for DLQ record eventId='{}'", eventId, e);
                }
            }

            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        } finally {
            MDC.clear();
        }
    }
}
