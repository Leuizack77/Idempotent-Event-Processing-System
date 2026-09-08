package com.example.eventprocessor.kafka.producer;

import com.example.eventprocessor.config.KafkaConfig;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Dedicated Kafka producer publishing domain events with eventId as record key.
 */
@Component
public class KafkaEventProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventProducer.class);

    private final KafkaTemplate<String, EventEnvelope> kafkaTemplate;

    public KafkaEventProducer(KafkaTemplate<String, EventEnvelope> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes an event envelope to the primary events topic.
     *
     * @param envelope The canonical event envelope to send.
     * @return CompletableFuture representing the broker send result.
     */
    public CompletableFuture<SendResult<String, EventEnvelope>> sendEvent(EventEnvelope envelope) {
        String eventId = envelope.eventId();
        String topic = KafkaConfig.TOPIC_EVENTS;

        log.info("Publishing event to Kafka: topic={}, eventId={}, eventType={}", topic, eventId, envelope.eventType());

        CompletableFuture<SendResult<String, EventEnvelope>> future = kafkaTemplate.send(topic, eventId, envelope);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish event to Kafka: topic={}, eventId={}, error={}", topic, eventId, ex.getMessage(), ex);
            } else if (result != null) {
                log.info("Kafka publish confirmed: topic={}, partition={}, offset={}, eventId={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        eventId);
            }
        });

        return future;
    }
}
