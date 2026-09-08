package com.example.eventprocessor.kafka.consumer;

import com.example.eventprocessor.config.KafkaConfig;
import com.example.eventprocessor.idempotency.IdempotencyEngine;
import com.example.eventprocessor.idempotency.ProcessingResult;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Dedicated Kafka consumer listening to the primary events topic.
 * Uses the IdempotencyEngine to guarantee exactly-once business execution.
 * Manages manual offset acknowledgment, ensuring acknowledgment occurs strictly
 * after successful processing or when safely skipping duplicates.
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
            @Payload EventEnvelope envelope,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        log.info("Kafka record received: topic={}, partition={}, offset={}, key={}, eventId={}, eventType={}",
                topic, partition, offset, key, envelope.eventId(), envelope.eventType());

        try {
            // Process through the durable PostgreSQL idempotency engine
            ProcessingResult result = idempotencyEngine.process(envelope);

            // Acknowledge offset for both successful completion and safely skipped duplicates
            if (result == ProcessingResult.SUCCESS || result == ProcessingResult.SKIPPED_DUPLICATE) {
                acknowledgment.acknowledge();
                log.info("Kafka offset acknowledged: topic={}, partition={}, offset={}, eventId={}, result={}",
                        topic, partition, offset, envelope.eventId(), result);
            }
        } catch (Exception e) {
            log.error("Processing failed for eventId={}. Offset will NOT be acknowledged.", envelope.eventId(), e);
            throw e; // Bubble exception so listener container triggers redelivery
        }
    }
}
