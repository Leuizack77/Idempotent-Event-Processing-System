package com.example.eventprocessor.kafka.producer;

import com.example.eventprocessor.config.KafkaConfig;
import com.example.eventprocessor.config.MdcFilter;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Dedicated Kafka producer publishing domain events with eventId as record key.
 * Propagates traceId and correlationId headers for distributed tracing.
 */
@Component
public class KafkaEventProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventProducer.class);

    private final KafkaTemplate<String, EventEnvelope> kafkaTemplate;

    public KafkaEventProducer(KafkaTemplate<String, EventEnvelope> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes an event envelope to the primary events topic with trace correlation headers.
     *
     * @param envelope The canonical event envelope to send.
     * @return CompletableFuture representing the broker send result.
     */
    public CompletableFuture<SendResult<String, EventEnvelope>> sendEvent(EventEnvelope envelope) {
        String eventId = envelope.eventId();
        String eventType = envelope.eventType();
        String topic = KafkaConfig.TOPIC_EVENTS;

        String traceId = MDC.get(MdcFilter.TRACE_ID_KEY);
        if (traceId == null || traceId.isBlank()) {
            traceId = MDC.get(MdcFilter.CORRELATION_ID_KEY);
        }
        if (traceId == null || traceId.isBlank()) {
            traceId = "tr-" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        }

        ProducerRecord<String, EventEnvelope> record = new ProducerRecord<>(topic, eventId, envelope);
        record.headers().add(MdcFilter.HEADER_TRACE_ID, traceId.getBytes(StandardCharsets.UTF_8));
        record.headers().add(MdcFilter.HEADER_CORRELATION_ID, traceId.getBytes(StandardCharsets.UTF_8));

        log.info("Publishing event to Kafka: topic={}, eventId={}, eventType={}", topic, eventId, eventType);

        final String finalTraceId = traceId;
        CompletableFuture<SendResult<String, EventEnvelope>> future = kafkaTemplate.send(record);

        future.whenComplete((result, ex) -> {
            MDC.put(MdcFilter.TRACE_ID_KEY, finalTraceId);
            MDC.put(MdcFilter.CORRELATION_ID_KEY, finalTraceId);
            MDC.put(MdcFilter.EVENT_ID_KEY, eventId);
            MDC.put(MdcFilter.EVENT_TYPE_KEY, eventType);
            try {
                if (ex != null) {
                    log.error("Failed to publish event to Kafka: topic={}, eventId={}, error={}", topic, eventId, ex.getMessage(), ex);
                } else if (result != null) {
                    log.info("Kafka publish confirmed: topic={}, partition={}, offset={}, eventId={}",
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset(),
                            eventId);
                }
            } finally {
                MDC.clear();
            }
        });

        return future;
    }
}
