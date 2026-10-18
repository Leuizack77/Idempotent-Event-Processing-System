package com.example.eventprocessor.kafka;

import com.example.eventprocessor.AbstractPostgresIntegrationTest;
import com.example.eventprocessor.config.KafkaConfig;
import com.example.eventprocessor.kafka.consumer.EventProcessingDelegate;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.kafka.producer.KafkaEventProducer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.example.eventprocessor.kafka.consumer.DefaultEventProcessingDelegate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class KafkaProducerConsumerIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private KafkaEventProducer kafkaEventProducer;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private DefaultEventProcessingDelegate eventProcessingDelegate;

    @BeforeEach
    void setUp() {
        eventProcessingDelegate.reset();
    }

    @Test
    @DisplayName("Test 1, 2, 3: Producer publishes event to 'events' topic with eventId as key and correct JSON envelope")
    void testProducerPublishesEventWithCorrectKeyAndEnvelope() throws Exception {
        String eventId = "EVT-KAFKA-PROD-001";
        String eventType = "PAYMENT_COMPLETED";
        Map<String, Object> payload = Map.of(
                "paymentId", "PAY-KAFKA-1",
                "userId", "USER-KAFKA-1",
                "amount", 100.0,
                "currency", "USD"
        );
        EventEnvelope envelope = EventEnvelope.of(eventId, eventType, payload);

        CompletableFuture<SendResult<String, EventEnvelope>> future = kafkaEventProducer.sendEvent(envelope);
        SendResult<String, EventEnvelope> result = future.get(10, TimeUnit.SECONDS);

        assertThat(result).isNotNull();
        assertThat(result.getRecordMetadata().topic()).isEqualTo(KafkaConfig.TOPIC_EVENTS);
        assertThat(result.getProducerRecord().key()).isEqualTo(eventId);
        assertThat(result.getProducerRecord().value().eventId()).isEqualTo(eventId);
        assertThat(result.getProducerRecord().value().eventType()).isEqualTo(eventType);
        assertThat(result.getProducerRecord().value().payload()).containsEntry("paymentId", "PAY-KAFKA-1");

        // Verify with an independent raw consumer that the broker actually stored key = eventId
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "raw-verification-group-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EventEnvelope.class.getName());

        try (Consumer<String, EventEnvelope> rawConsumer = new DefaultKafkaConsumerFactory<String, EventEnvelope>(consumerProps).createConsumer()) {
            rawConsumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC_EVENTS));
            ConsumerRecords<String, EventEnvelope> records = rawConsumer.poll(Duration.ofSeconds(10));
            boolean recordFound = false;
            for (ConsumerRecord<String, EventEnvelope> record : records) {
                if (eventId.equals(record.key())) {
                    recordFound = true;
                    assertThat(record.value().eventId()).isEqualTo(eventId);
                    assertThat(record.value().eventType()).isEqualTo(eventType);
                    assertThat(record.value().payload()).containsEntry("paymentId", "PAY-KAFKA-1");
                    break;
                }
            }
            assertThat(recordFound).isTrue();
        }
    }

    @Test
    @DisplayName("Test 4 & 5: Kafka consumer receives and successfully processes message via delegate")
    void testConsumerReceivesAndProcessesMessage() {
        String eventId = "EVT-KAFKA-CONS-002";
        Map<String, Object> payload = Map.of(
                "orderId", "ORD-KAFKA-2",
                "customerId", "CUST-KAFKA-2",
                "totalAmount", 250.0,
                "itemCount", 2
        );
        EventEnvelope envelope = EventEnvelope.of(eventId, "ORDER_CREATED", payload);

        AtomicInteger processedCount = new AtomicInteger(0);
        eventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                processedCount.incrementAndGet();
            }
        });

        kafkaEventProducer.sendEvent(envelope);

        // Await consumer processing
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(processedCount.get()).isEqualTo(1));
    }

    @Test
    @DisplayName("Test 6 & 7: Processing failure before acknowledgment enables redelivery")
    void testFailureBeforeAcknowledgmentAllowsRedelivery() {
        String eventId = "EVT-KAFKA-RETRY-003";
        Map<String, Object> payload = Map.of(
                "paymentId", "PAY-RETRY-1",
                "userId", "USER-RETRY-1",
                "amount", 50.0,
                "currency", "USD"
        );
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", payload);

        AtomicInteger attempts = new AtomicInteger(0);

        // Fail on first attempt (simulating processing error prior to acknowledgment.acknowledge()),
        // then succeed on subsequent attempt
        eventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                int attempt = attempts.incrementAndGet();
                if (attempt == 1) {
                    throw new RuntimeException("Simulated transient processing error before ack");
                }
            }
        });

        kafkaEventProducer.sendEvent(envelope);

        // Verify redelivery: delegate is invoked more than once due to failure on attempt 1
        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(attempts.get()).isGreaterThanOrEqualTo(2));
    }

    @Test
    @DisplayName("Test 8: REST Ingestion triggers Kafka publish and end-to-end consumption")
    void testRestIngestionPublishesToKafkaAndConsumes() throws Exception {
        String eventId = "EVT-REST-KAFKA-004";
        String requestJson = """
                {
                    "eventId": "%s",
                    "eventType": "PAYMENT_COMPLETED",
                    "payload": {
                        "paymentId": "PAY-E2E-1",
                        "userId": "USER-E2E-1",
                        "amount": 99.99,
                        "currency": "USD"
                    }
                }
                """.formatted(eventId);

        AtomicInteger restConsumed = new AtomicInteger(0);
        eventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                restConsumed.incrementAndGet();
            }
        });

        mockMvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").value(eventId))
                .andExpect(jsonPath("$.status").value("SUBMITTED"));

        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(restConsumed.get()).isEqualTo(1));
    }
}
