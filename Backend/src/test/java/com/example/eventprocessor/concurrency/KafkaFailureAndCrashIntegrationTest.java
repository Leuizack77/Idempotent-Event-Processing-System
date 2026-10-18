package com.example.eventprocessor.concurrency;

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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class KafkaFailureAndCrashIntegrationTest extends AbstractPostgresIntegrationTest {

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
        paymentRepository.deleteAll();
        processedEventRepository.deleteAll();
    }

    private Consumer<String, EventEnvelope> createDlqConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-test-verifier-" + UUID.randomUUID());
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
    @DisplayName("SCENARIO 4 — Kafka Consumer Crash / Uncommitted Offset / Redelivery: processing occurs before offset ACK, redelivery succeeds")
    void testConsumerFailureBeforeAckEnablesBrokerRedelivery() {
        String eventId = "EVT-KAFKA-CRASH-001";
        String paymentId = "PAY-KAFKA-CRASH-001";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-CRASH-1",
                "amount", 150.0,
                "currency", "USD"
        ));

        AtomicInteger deliveryAttempts = new AtomicInteger(0);

        // Fail on the first delivery attempt before acknowledgment can occur
        eventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                int attempt = deliveryAttempts.incrementAndGet();
                if (attempt == 1) {
                    throw new CannotAcquireLockException("Simulated transient processing failure prior to Kafka offset commit");
                }
            }
        });

        kafkaEventProducer.sendEvent(envelope);

        // Await redelivery and eventual successful completion
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(deliveryAttempts.get()).isGreaterThanOrEqualTo(2);

            Optional<PaymentEntity> paymentOpt = paymentRepository.findByPaymentId(paymentId);
            assertThat(paymentOpt).isPresent();
            assertThat(paymentOpt.get().getAmount()).isEqualByComparingTo(new BigDecimal("150.0"));

            Optional<ProcessedEventEntity> eventOpt = processedEventRepository.findByEventId(eventId);
            assertThat(eventOpt).isPresent();
            assertThat(eventOpt.get().getStatus()).isEqualTo(EventStatus.COMPLETED);
            assertThat(eventOpt.get().getRetryCount()).isGreaterThanOrEqualTo(1);
        });

        // Exactly 1 payment must be created despite the crash/retry
        long paymentCount = paymentRepository.findAll().stream()
                .filter(p -> paymentId.equals(p.getPaymentId()))
                .count();
        assertThat(paymentCount).isEqualTo(1);

        // Verify message was NOT routed to DLQ
        try (Consumer<String, EventEnvelope> dlqConsumer = createDlqConsumer()) {
            ConsumerRecords<String, EventEnvelope> records = dlqConsumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, EventEnvelope> record : records) {
                assertThat(record.key()).isNotEqualTo(eventId);
            }
        }
    }

    @Test
    @DisplayName("SCENARIO 5 — Multi-Partition Concurrency + Rebalancing: 30 events across 3 partitions survive consumer rebalancing")
    void testMultiPartitionConcurrentDeliveryAndRebalancing() {
        int totalEvents = 30;
        List<String> eventIds = new ArrayList<>();
        List<String> paymentIds = new ArrayList<>();

        for (int i = 1; i <= totalEvents; i++) {
            eventIds.add("EVT-KAFKA-REBAL-" + i);
            paymentIds.add("PAY-KAFKA-REBAL-" + i);
        }

        // Publish events to Kafka topic (topic has 3 partitions)
        for (int i = 0; i < totalEvents; i++) {
            EventEnvelope envelope = EventEnvelope.of(eventIds.get(i), "PAYMENT_COMPLETED", Map.of(
                    "paymentId", paymentIds.get(i),
                    "userId", "USER-REBAL-" + i,
                    "amount", 10.0 + i,
                    "currency", "USD"
            ));
            kafkaEventProducer.sendEvent(envelope);

            // Mid-stream, simulate group rebalance by creating a secondary consumer in the same group
            if (i == 15) {
                Map<String, Object> props = new HashMap<>();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "event-processor-group");
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
                props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
                props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EventEnvelope.class.getName());

                try (Consumer<String, EventEnvelope> secondaryConsumer =
                             new DefaultKafkaConsumerFactory<String, EventEnvelope>(props).createConsumer()) {
                    secondaryConsumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC_EVENTS));
                    secondaryConsumer.poll(Duration.ofMillis(300));
                    // Rebalance triggered on join and rebalance triggered again on close
                }
            }
        }

        // Verify all 30 events are eventually processed and COMPLETED
        await().atMost(45, TimeUnit.SECONDS).untilAsserted(() -> {
            long completedCount = processedEventRepository.findAll().stream()
                    .filter(e -> e.getStatus() == EventStatus.COMPLETED)
                    .count();
            assertThat(completedCount).isGreaterThanOrEqualTo(totalEvents);

            long totalPayments = paymentRepository.count();
            assertThat(totalPayments).isGreaterThanOrEqualTo(totalEvents);
        });

        // Verify each individual payment exists exactly once
        for (String paymentId : paymentIds) {
            long count = paymentRepository.findAll().stream()
                    .filter(p -> paymentId.equals(p.getPaymentId()))
                    .count();
            assertThat(count).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("SCENARIO 6 — 20+ Rapid Duplicates Through Real Kafka: exactly 1 business mutation, all duplicates safely ACKed")
    void testRapidDuplicatesThroughRealKafka() {
        String eventId = "EVT-KAFKA-BURST-DUP-001";
        String paymentId = "PAY-KAFKA-BURST-DUP-001";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-BURST",
                "amount", 888.0,
                "currency", "USD"
        ));

        int duplicateCount = 25;
        List<CompletableFuture<?>> futures = new ArrayList<>();

        // Fire 25 duplicate records into Kafka broker rapidly without waiting
        for (int i = 0; i < duplicateCount; i++) {
            futures.add(kafkaEventProducer.sendEvent(envelope));
        }

        // Wait for all messages to be delivered to Kafka broker
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Await consumption of all duplicates
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Optional<ProcessedEventEntity> eventOpt = processedEventRepository.findByEventId(eventId);
            assertThat(eventOpt).isPresent();
            assertThat(eventOpt.get().getStatus()).isEqualTo(EventStatus.COMPLETED);

            long paymentCount = paymentRepository.findAll().stream()
                    .filter(p -> paymentId.equals(p.getPaymentId()))
                    .count();
            assertThat(paymentCount).isEqualTo(1);
        });

        // Settle briefly and confirm payment count remains strictly 1
        await().pollDelay(1, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            long paymentCount = paymentRepository.findAll().stream()
                    .filter(p -> paymentId.equals(p.getPaymentId()))
                    .count();
            assertThat(paymentCount).isEqualTo(1);
        });

        // Verify zero records were published to DLQ for this event
        try (Consumer<String, EventEnvelope> dlqConsumer = createDlqConsumer()) {
            ConsumerRecords<String, EventEnvelope> records = dlqConsumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, EventEnvelope> record : records) {
                assertThat(record.key()).isNotEqualTo(eventId);
            }
        }
    }
}
