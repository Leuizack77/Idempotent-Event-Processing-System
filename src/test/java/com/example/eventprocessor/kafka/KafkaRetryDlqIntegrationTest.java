package com.example.eventprocessor.kafka;

import com.example.eventprocessor.AbstractPostgresIntegrationTest;
import com.example.eventprocessor.config.KafkaConfig;
import com.example.eventprocessor.config.KafkaRetryProperties;
import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.entity.PaymentEntity;
import com.example.eventprocessor.entity.ProcessedEventEntity;
import com.example.eventprocessor.kafka.consumer.DefaultEventProcessingDelegate;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.kafka.producer.KafkaEventProducer;
import com.example.eventprocessor.repository.PaymentRepository;
import com.example.eventprocessor.repository.ProcessedEventRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class KafkaRetryDlqIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private KafkaEventProducer kafkaEventProducer;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private DefaultEventProcessingDelegate eventProcessingDelegate;

    @Autowired
    private KafkaRetryProperties retryProperties;

    @BeforeEach
    void setUp() {
        eventProcessingDelegate.reset();
    }

    private Consumer<String, EventEnvelope> createDlqConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-verifier-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EventEnvelope.class.getName());

        Consumer<String, EventEnvelope> consumer = new DefaultKafkaConsumerFactory<String, EventEnvelope>(props).createConsumer();
        consumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC_EVENTS_DLQ));
        return consumer;
    }

    @Test
    @DisplayName("TEST 1 — Successful first attempt: valid event commits business mutation, status COMPLETED, no DLQ")
    void testSuccessfulFirstAttempt() {
        String eventId = "EVT-RETRY-SUCCESS-001";
        String paymentId = "PAY-RETRY-101";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-101",
                "amount", 150.0,
                "currency", "USD"
        ));

        kafkaEventProducer.sendEvent(envelope);

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<PaymentEntity> paymentOpt = paymentRepository.findByPaymentId(paymentId);
            assertThat(paymentOpt).isPresent();
            assertThat(paymentOpt.get().getAmount()).isEqualByComparingTo(new BigDecimal("150.0"));

            Optional<ProcessedEventEntity> eventOpt = processedEventRepository.findByEventId(eventId);
            assertThat(eventOpt).isPresent();
            assertThat(eventOpt.get().getStatus()).isEqualTo(EventStatus.COMPLETED);
        });

        // Verify no record published to DLQ for this event
        try (Consumer<String, EventEnvelope> dlqConsumer = createDlqConsumer()) {
            ConsumerRecords<String, EventEnvelope> records = dlqConsumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, EventEnvelope> record : records) {
                assertThat(record.key()).isNotEqualTo(eventId);
            }
        }
    }

    @Test
    @DisplayName("TEST 2 — Transient failure then success: retry occurs, exactly 1 business mutation, status COMPLETED, no DLQ")
    void testTransientFailureThenSuccess() {
        String eventId = "EVT-RETRY-TRANS-002";
        String paymentId = "PAY-RETRY-202";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-202",
                "amount", 200.0,
                "currency", "USD"
        ));

        AtomicInteger attempts = new AtomicInteger(0);
        eventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                int attempt = attempts.incrementAndGet();
                if (attempt == 1) {
                    throw new CannotAcquireLockException("Simulated transient database lock contention");
                }
            }
        });

        kafkaEventProducer.sendEvent(envelope);

        // Verify retry occurred (attempts >= 2) and succeeded
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(attempts.get()).isGreaterThanOrEqualTo(2);

            Optional<PaymentEntity> paymentOpt = paymentRepository.findByPaymentId(paymentId);
            assertThat(paymentOpt).isPresent();

            Optional<ProcessedEventEntity> eventOpt = processedEventRepository.findByEventId(eventId);
            assertThat(eventOpt).isPresent();
            assertThat(eventOpt.get().getStatus()).isEqualTo(EventStatus.COMPLETED);
        });

        // Verify exactly one payment was persisted
        long paymentCount = paymentRepository.findAll().stream()
                .filter(p -> paymentId.equals(p.getPaymentId()))
                .count();
        assertThat(paymentCount).isEqualTo(1);
    }

    @Test
    @DisplayName("TEST 3 — Retry exhaustion: transient failure on all attempts, published to events.dlq, status FAILED")
    void testRetryExhaustionRoutesToDlq() {
        String eventId = "EVT-RETRY-EXHAUST-003";
        String paymentId = "PAY-RETRY-303";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-303",
                "amount", 300.0,
                "currency", "USD"
        ));

        AtomicInteger attempts = new AtomicInteger(0);
        eventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                attempts.incrementAndGet();
                throw new CannotAcquireLockException("Persistent transient database error");
            }
        });

        kafkaEventProducer.sendEvent(envelope);

        // Await retry exhaustion up to configured max attempts (3)
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(attempts.get()).isGreaterThanOrEqualTo(retryProperties.getMaxAttempts());

            Optional<ProcessedEventEntity> eventOpt = processedEventRepository.findByEventId(eventId);
            assertThat(eventOpt).isPresent();
            assertThat(eventOpt.get().getStatus()).isEqualTo(EventStatus.FAILED);

            // Business mutation was rolled back / not persisted
            assertThat(paymentRepository.findByPaymentId(paymentId)).isEmpty();
        });

        // Verify message was delivered to DLQ
        try (Consumer<String, EventEnvelope> dlqConsumer = createDlqConsumer()) {
            boolean foundInDlq = false;
            long deadline = System.currentTimeMillis() + 15000;
            while (System.currentTimeMillis() < deadline && !foundInDlq) {
                ConsumerRecords<String, EventEnvelope> records = dlqConsumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, EventEnvelope> record : records) {
                    if (eventId.equals(record.key())) {
                        foundInDlq = true;
                        assertThat(record.value().eventId()).isEqualTo(eventId);
                        break;
                    }
                }
            }
            assertThat(foundInDlq).isTrue();
        }
    }

    @Test
    @DisplayName("TEST 4 — Non-retryable exception: invalid payload skips retries, sent directly to DLQ, status FAILED")
    void testNonRetryableExceptionRoutesDirectlyToDlq() {
        String eventId = "EVT-RETRY-FATAL-004";
        String paymentId = "PAY-RETRY-404";
        // Negative amount is deterministically fatal (InvalidEventPayloadException)
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-404",
                "amount", -100.0,
                "currency", "USD"
        ));

        AtomicInteger invocations = new AtomicInteger(0);
        eventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                invocations.incrementAndGet();
            }
        });

        kafkaEventProducer.sendEvent(envelope);

        // Await direct routing to DLQ without retries
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            // Invoked at most 1 time (no retry loop)
            assertThat(invocations.get()).isLessThanOrEqualTo(1);

            Optional<ProcessedEventEntity> eventOpt = processedEventRepository.findByEventId(eventId);
            assertThat(eventOpt).isPresent();
            assertThat(eventOpt.get().getStatus()).isEqualTo(EventStatus.FAILED);
            assertThat(eventOpt.get().getErrorMessage()).contains("must be positive");

            assertThat(paymentRepository.findByPaymentId(paymentId)).isEmpty();
        });

        // Verify published directly to events.dlq
        try (Consumer<String, EventEnvelope> dlqConsumer = createDlqConsumer()) {
            boolean foundInDlq = false;
            long deadline = System.currentTimeMillis() + 15000;
            while (System.currentTimeMillis() < deadline && !foundInDlq) {
                ConsumerRecords<String, EventEnvelope> records = dlqConsumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, EventEnvelope> record : records) {
                    if (eventId.equals(record.key())) {
                        foundInDlq = true;
                        break;
                    }
                }
            }
            assertThat(foundInDlq).isTrue();
        }
    }

    @Test
    @DisplayName("TEST 5 — Duplicate after successful processing: duplicate skipped, exactly 1 business mutation, no DLQ")
    void testDuplicateAfterSuccessfulProcessing() {
        String eventId = "EVT-RETRY-DUP-005";
        String paymentId = "PAY-RETRY-505";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-505",
                "amount", 550.0,
                "currency", "USD"
        ));

        kafkaEventProducer.sendEvent(envelope);

        // Await first successful completion
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(paymentRepository.findByPaymentId(paymentId)).isPresent();
            assertThat(processedEventRepository.findByEventId(eventId).get().getStatus()).isEqualTo(EventStatus.COMPLETED);
        });

        // Redeliver identical event
        kafkaEventProducer.sendEvent(envelope);

        // Await verification that duplicate was skipped and no DLQ message was created
        await().pollDelay(1, TimeUnit.SECONDS).atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            long count = paymentRepository.findAll().stream()
                    .filter(p -> paymentId.equals(p.getPaymentId()))
                    .count();
            assertThat(count).isEqualTo(1);
            assertThat(processedEventRepository.findByEventId(eventId).get().getStatus()).isEqualTo(EventStatus.COMPLETED);
        });

        try (Consumer<String, EventEnvelope> dlqConsumer = createDlqConsumer()) {
            ConsumerRecords<String, EventEnvelope> records = dlqConsumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, EventEnvelope> record : records) {
                assertThat(record.key()).isNotEqualTo(eventId);
            }
        }
    }

    @Test
    @DisplayName("TEST 6 — Retry preserves idempotency: retry after reservation recovers safely, final status COMPLETED")
    void testRetryPreservesIdempotency() {
        String eventId = "EVT-RETRY-IDEMP-006";
        String paymentId = "PAY-RETRY-606";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-606",
                "amount", 660.0,
                "currency", "USD"
        ));

        AtomicInteger attempt = new AtomicInteger(0);
        eventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                if (attempt.incrementAndGet() == 1) {
                    throw new CannotAcquireLockException("Transient lock error in business delegate");
                }
            }
        });

        kafkaEventProducer.sendEvent(envelope);

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(attempt.get()).isGreaterThanOrEqualTo(2);

            Optional<PaymentEntity> paymentOpt = paymentRepository.findByPaymentId(paymentId);
            assertThat(paymentOpt).isPresent();

            Optional<ProcessedEventEntity> eventOpt = processedEventRepository.findByEventId(eventId);
            assertThat(eventOpt).isPresent();
            assertThat(eventOpt.get().getStatus()).isEqualTo(EventStatus.COMPLETED);
            assertThat(eventOpt.get().getRetryCount()).isGreaterThanOrEqualTo(1);
        });

        long count = paymentRepository.findAll().stream()
                .filter(p -> paymentId.equals(p.getPaymentId()))
                .count();
        assertThat(count).isEqualTo(1);
    }

    @Test
    @DisplayName("TEST 7 — DLQ diagnostics: DLQ record contains original key, payload, and diagnostic headers")
    void testDlqDiagnosticMetadata() {
        String eventId = "EVT-RETRY-DIAG-007";
        String paymentId = "PAY-RETRY-707";
        // Invalid payload causes immediate DLQ routing
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-707",
                "amount", -25.0,
                "currency", "USD"
        ));

        kafkaEventProducer.sendEvent(envelope);

        // Await processing failure and DLQ routing
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<ProcessedEventEntity> eventOpt = processedEventRepository.findByEventId(eventId);
            assertThat(eventOpt).isPresent();
            assertThat(eventOpt.get().getStatus()).isEqualTo(EventStatus.FAILED);
        });

        try (Consumer<String, EventEnvelope> dlqConsumer = createDlqConsumer()) {
            ConsumerRecord<String, EventEnvelope> matching = null;
            long deadline = System.currentTimeMillis() + 15000;
            while (System.currentTimeMillis() < deadline && matching == null) {
                ConsumerRecords<String, EventEnvelope> records = dlqConsumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, EventEnvelope> record : records) {
                    if (eventId.equals(record.key())) {
                        matching = record;
                        break;
                    }
                }
            }

            assertThat(matching).isNotNull();
            assertThat(matching.key()).isEqualTo(eventId);
            assertThat(matching.value().eventId()).isEqualTo(eventId);
            assertThat(matching.value().eventType()).isEqualTo("PAYMENT_COMPLETED");
            assertThat(matching.topic()).isEqualTo(KafkaConfig.TOPIC_EVENTS_DLQ);

            // Verify diagnostic headers
            Map<String, String> headersMap = new HashMap<>();
            for (Header h : matching.headers()) {
                headersMap.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
            }

            // Check custom diagnostic headers
            assertThat(headersMap).containsKey("X-Exception-Message");
            assertThat(headersMap.get("X-Exception-Message")).contains("must be positive");
            assertThat(headersMap).containsKey("X-Original-Topic");
            assertThat(headersMap.get("X-Original-Topic")).isEqualTo(KafkaConfig.TOPIC_EVENTS);
            assertThat(headersMap).containsKey("X-Original-Partition");
            assertThat(headersMap).containsKey("X-Original-Offset");
        }
    }
}
