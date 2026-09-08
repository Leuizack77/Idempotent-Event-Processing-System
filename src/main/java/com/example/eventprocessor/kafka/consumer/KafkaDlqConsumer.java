package com.example.eventprocessor.kafka.consumer;

import com.example.eventprocessor.config.KafkaConfig;
import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Dedicated consumer listening to the Dead Letter Queue (events.dlq).
 * Captures terminal event failures, records diagnostics, updates PostgreSQL status to FAILED,
 * and preserves poison pills for audit and administrative inspection.
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
            @Payload(required = false) EventEnvelope envelope,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(value = "X-Exception-Message", required = false) String exceptionMessage,
            @Header(value = "X-Retry-Count", required = false) String retryCount,
            @Header(value = KafkaHeaders.RECEIVED_TOPIC, required = false) String topic,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition,
            @Header(value = KafkaHeaders.OFFSET, required = false) Long offset,
            Acknowledgment acknowledgment
    ) {
        String eventId = envelope != null ? envelope.eventId() : key;
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
    }
}
