package com.example.eventprocessor.kafka.consumer;

import com.example.eventprocessor.config.KafkaConfig;
import com.example.eventprocessor.config.MdcFilter;
import com.example.eventprocessor.idempotency.IdempotencyEngine;
import com.example.eventprocessor.idempotency.ProcessingResult;
import com.example.eventprocessor.kafka.model.EventEnvelope;
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
import java.util.UUID;

/**
 * Dedicated Kafka consumer listening to the primary events topic.
 * Uses the IdempotencyEngine to guarantee exactly-once business execution.
 * Manages manual offset acknowledgment, ensuring acknowledgment occurs strictly
 * after successful processing or when safely skipping duplicates.
 * Populates MDC context for structured logging across execution steps.
 */
@Component
public class KafkaEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventConsumer.class);

    private final IdempotencyEngine idempotencyEngine;

    public KafkaEventConsumer(IdempotencyEngine idempotencyEngine) {
        this.idempotencyEngine = idempotencyEngine;
    }

    @KafkaListener(
            topics = KafkaConfig.TOPIC_EVENTS,
            groupId = "${spring.kafka.consumer.group-id:event-processor-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            org.apache.kafka.clients.consumer.ConsumerRecord<String, EventEnvelope> record,
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

        MDC.put(MdcFilter.TRACE_ID_KEY, traceId);
        MDC.put(MdcFilter.CORRELATION_ID_KEY, traceId);
        MDC.put(MdcFilter.EVENT_ID_KEY, envelope != null ? envelope.eventId() : key);
        MDC.put(MdcFilter.EVENT_TYPE_KEY, envelope != null ? envelope.eventType() : "UNKNOWN");

        try {
            log.info("Kafka record received: topic={}, partition={}, offset={}, key={}, eventId={}, eventType={}",
                    topic, partition, offset, key,
                    envelope != null ? envelope.eventId() : key,
                    envelope != null ? envelope.eventType() : "UNKNOWN");

            // Process through the durable PostgreSQL idempotency engine
            ProcessingResult result = idempotencyEngine.process(envelope);

            // Acknowledge offset for both successful completion and safely skipped duplicates
            if (result == ProcessingResult.SUCCESS || result == ProcessingResult.SKIPPED_DUPLICATE) {
                acknowledgment.acknowledge();
                log.info("Kafka offset acknowledged: topic={}, partition={}, offset={}, eventId={}, result={}",
                        topic, partition, offset,
                        envelope != null ? envelope.eventId() : key,
                        result);
            }
        } catch (Exception e) {
            log.error("Processing failed for eventId={}. Offset will NOT be acknowledged.",
                    envelope != null ? envelope.eventId() : key, e);
            throw e; // Bubble exception so listener container triggers redelivery
        } finally {
            MDC.clear();
        }
    }
}
