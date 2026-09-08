package com.example.eventprocessor.config;

import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.exception.StaleProcessingLeaseException;
import com.example.eventprocessor.kafka.consumer.KafkaRetryClassifier;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.repository.ProcessedEventRepository;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    public static final String TOPIC_EVENTS = "events";
    public static final String TOPIC_EVENTS_RETRY = "events.retry";
    public static final String TOPIC_EVENTS_DLQ = "events.dlq";

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:event-processor-group}")
    private String consumerGroupId;

    // --- Topic Infrastructure Beans ---

    @Bean
    public NewTopic eventsTopic() {
        return TopicBuilder.name(TOPIC_EVENTS)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic eventsRetryTopic() {
        return TopicBuilder.name(TOPIC_EVENTS_RETRY)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic eventsDlqTopic() {
        return TopicBuilder.name(TOPIC_EVENTS_DLQ)
                .partitions(3)
                .replicas(1)
                .build();
    }

    // --- Producer Configuration ---

    @Bean
    public ProducerFactory<String, EventEnvelope> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, 10);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, EventEnvelope> kafkaTemplate(ProducerFactory<String, EventEnvelope> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    // --- Dead Letter Queue & Error Handling Configuration ---

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, EventEnvelope> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(TOPIC_EVENTS_DLQ, -1)
        );

        recoverer.addHeadersFunction((record, exception) -> {
            RecordHeaders headers = new RecordHeaders();
            if (exception != null) {
                Throwable root = org.springframework.core.NestedExceptionUtils.getMostSpecificCause(exception);
                String msg = root != null && root.getMessage() != null ? root.getMessage() : exception.getMessage();
                headers.add("X-Exception-Message", msg.getBytes(StandardCharsets.UTF_8));
                headers.add("X-Exception-Class", root.getClass().getName().getBytes(StandardCharsets.UTF_8));
            }
            if (record.topic() != null) {
                headers.add("X-Original-Topic", record.topic().getBytes(StandardCharsets.UTF_8));
            }
            headers.add("X-Original-Partition", String.valueOf(record.partition()).getBytes(StandardCharsets.UTF_8));
            headers.add("X-Original-Offset", String.valueOf(record.offset()).getBytes(StandardCharsets.UTF_8));
            headers.add("X-Failed-Timestamp", Instant.now().toString().getBytes(StandardCharsets.UTF_8));

            org.apache.kafka.common.header.Header deliveryAttemptHeader = record.headers().lastHeader(KafkaHeaders.DELIVERY_ATTEMPT);
            if (deliveryAttemptHeader != null) {
                headers.add("X-Retry-Count", deliveryAttemptHeader.value());
            } else {
                headers.add("X-Retry-Count", "3".getBytes(StandardCharsets.UTF_8));
            }
            return headers;
        });

        return recoverer;
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler(
            DeadLetterPublishingRecoverer deadLetterPublishingRecoverer,
            KafkaRetryProperties retryProperties,
            ProcessedEventRepository processedEventRepository,
            KafkaRetryClassifier retryClassifier) {

        ExponentialBackOff backOff = new ExponentialBackOff(
                retryProperties.getInitialInterval().toMillis(),
                retryProperties.getMultiplier()
        );
        backOff.setMaxInterval(retryProperties.getMaxInterval().toMillis());
        backOff.setMaxAttempts(retryProperties.getMaxAttempts());

        ConsumerRecordRecoverer compositeRecoverer = (record, exception) -> {
            log.warn("Retries exhausted or fatal failure encountered. Publishing to DLQ: key={}, topic={}, partition={}, offset={}, exception={}",
                    record.key(), record.topic(), record.partition(), record.offset(),
                    exception != null ? exception.getMessage() : "none");

            // 1. Publish to events.dlq with diagnostic headers
            deadLetterPublishingRecoverer.accept(record, exception);

            // 2. Mark event status as FAILED in PostgreSQL (if not fenced out)
            markFailedOnRecovery(record, exception, processedEventRepository);
        };

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(compositeRecoverer, backOff);
        errorHandler.setCommitRecovered(true);
        errorHandler.setAckAfterHandle(true);

        // Register non-retryable exceptions to route directly to DLQ without futile retries
        for (Class<? extends Exception> nonRetryable : retryClassifier.getNonRetryableExceptions()) {
            errorHandler.addNotRetryableExceptions(nonRetryable);
        }

        return errorHandler;
    }

    private void markFailedOnRecovery(
            ConsumerRecord<?, ?> record,
            Exception exception,
            ProcessedEventRepository processedEventRepository) {
        // Stale fenced-out workers must not overwrite status to FAILED
        if (exception instanceof StaleProcessingLeaseException ||
                (exception != null && exception.getCause() instanceof StaleProcessingLeaseException)) {
            log.warn("Fenced-out worker recovered. Skipping status update to preserve newer owner state.");
            return;
        }

        String extractedEventId = null;
        if (record.value() instanceof EventEnvelope env) {
            extractedEventId = env.eventId();
        } else if (record.key() instanceof String k) {
            extractedEventId = k;
        }
        final String eventId = extractedEventId;

        if (eventId != null) {
            String errorMsg;
            if (exception != null) {
                Throwable root = org.springframework.core.NestedExceptionUtils.getMostSpecificCause(exception);
                errorMsg = root != null && root.getMessage() != null ? root.getMessage() : exception.getMessage();
            } else {
                errorMsg = "Retries exhausted or fatal failure";
            }
            try {
                processedEventRepository.findByEventId(eventId).ifPresent(entity -> {
                    if (entity.getStatus() != EventStatus.COMPLETED) {
                        entity.setStatus(EventStatus.FAILED);
                        entity.setErrorMessage(errorMsg);
                        entity.setUpdatedAt(Instant.now());
                        processedEventRepository.save(entity);
                        log.info("Durable status for eventId='{}' updated to FAILED upon DLQ routing", eventId);
                    }
                });
            } catch (Exception e) {
                log.error("Failed to update status to FAILED for eventId='{}'", eventId, e);
            }
        }
    }

    // --- Consumer Configuration with Manual Acknowledgment ---

    @Bean
    public ConsumerFactory<String, EventEnvelope> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Deserializer configuration with ErrorHandlingDeserializer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EventEnvelope.class.getName());

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, EventEnvelope> kafkaListenerContainerFactory(
            ConsumerFactory<String, EventEnvelope> consumerFactory,
            DefaultErrorHandler defaultErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, EventEnvelope> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setDeliveryAttemptHeader(true);
        factory.setCommonErrorHandler(defaultErrorHandler);
        return factory;
    }
}
