package com.example.eventprocessor.processing;

import com.example.eventprocessor.AbstractPostgresIntegrationTest;
import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.entity.OrderEntity;
import com.example.eventprocessor.entity.PaymentEntity;
import com.example.eventprocessor.entity.ProcessedEventEntity;
import com.example.eventprocessor.exception.DuplicateBusinessEntityException;
import com.example.eventprocessor.exception.EventProcessingInProgressException;
import com.example.eventprocessor.exception.InvalidEventPayloadException;
import com.example.eventprocessor.exception.StaleProcessingLeaseException;
import com.example.eventprocessor.idempotency.IdempotencyEngine;
import com.example.eventprocessor.idempotency.ProcessingResult;
import com.example.eventprocessor.kafka.consumer.DefaultEventProcessingDelegate;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.repository.OrderRepository;
import com.example.eventprocessor.repository.PaymentRepository;
import com.example.eventprocessor.repository.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionalBusinessProcessingIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private IdempotencyEngine idempotencyEngine;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private TransactionalBusinessExecutor transactionalBusinessExecutor;

    @Autowired
    private DefaultEventProcessingDelegate defaultEventProcessingDelegate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        defaultEventProcessingDelegate.reset();
        orderRepository.deleteAll();
        paymentRepository.deleteAll();
        processedEventRepository.deleteAll();
    }

    @Test
    @DisplayName("Test 1 â€” Payment Success: PAYMENT_COMPLETED persists PaymentEntity and marks processed_events COMPLETED atomically")
    void testPaymentCompletedSuccess() {
        String eventId = "EVT-PAY-001";
        String paymentId = "PAY-1001";
        String userId = "USER-4455";
        BigDecimal amount = new BigDecimal("150.75");
        String currency = "USD";

        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", userId,
                "amount", 150.75,
                "currency", currency
        ));

        ProcessingResult result = idempotencyEngine.process(envelope);
        assertThat(result).isEqualTo(ProcessingResult.SUCCESS);

        // Verify PaymentEntity exists in payments table
        Optional<PaymentEntity> paymentOpt = paymentRepository.findByPaymentId(paymentId);
        assertThat(paymentOpt).isPresent();
        PaymentEntity payment = paymentOpt.get();
        assertThat(payment.getPaymentId()).isEqualTo(paymentId);
        assertThat(payment.getUserId()).isEqualTo(userId);
        assertThat(payment.getAmount()).isEqualByComparingTo(amount);
        assertThat(payment.getCurrency()).isEqualTo(currency);
        assertThat(payment.getCreatedAt()).isNotNull();

        // Verify processed_events record is COMPLETED
        Optional<ProcessedEventEntity> eventOpt = processedEventRepository.findByEventId(eventId);
        assertThat(eventOpt).isPresent();
        ProcessedEventEntity event = eventOpt.get();
        assertThat(event.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(event.getProcessedAt()).isNotNull();
        assertThat(event.getErrorMessage()).isNull();
    }

    @Test
    @DisplayName("Test 2 â€” Payment Duplicate: At-least-once redelivery results in exactly-once business mutation")
    void testPaymentDuplicateSkipsExecution() {
        String eventId = "EVT-PAY-DUP-002";
        String paymentId = "PAY-DUP-2002";

        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-9900",
                "amount", 89.99,
                "currency", "USD"
        ));

        // First delivery: processes successfully
        ProcessingResult firstResult = idempotencyEngine.process(envelope);
        assertThat(firstResult).isEqualTo(ProcessingResult.SUCCESS);
        assertThat(paymentRepository.count()).isEqualTo(1);

        // Second delivery (duplicate): returns SKIPPED_DUPLICATE without re-executing business logic
        ProcessingResult secondResult = idempotencyEngine.process(envelope);
        assertThat(secondResult).isEqualTo(ProcessingResult.SKIPPED_DUPLICATE);

        // Crucial invariant: Exactly 1 payment record remains in PostgreSQL
        assertThat(paymentRepository.count()).isEqualTo(1);
        Optional<PaymentEntity> paymentOpt = paymentRepository.findByPaymentId(paymentId);
        assertThat(paymentOpt).isPresent();

        ProcessedEventEntity event = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(event.getStatus()).isEqualTo(EventStatus.COMPLETED);
    }

    @Test
    @DisplayName("Test 3 â€” Order Success: ORDER_CREATED persists OrderEntity and marks processed_events COMPLETED atomically")
    void testOrderCreatedSuccess() {
        String eventId = "EVT-ORD-003";
        String orderId = "ORD-3001";
        String customerId = "CUST-1122";
        BigDecimal totalAmount = new BigDecimal("349.50");
        Integer itemCount = 4;

        EventEnvelope envelope = EventEnvelope.of(eventId, "ORDER_CREATED", Map.of(
                "orderId", orderId,
                "customerId", customerId,
                "totalAmount", 349.50,
                "itemCount", 4
        ));

        ProcessingResult result = idempotencyEngine.process(envelope);
        assertThat(result).isEqualTo(ProcessingResult.SUCCESS);

        // Verify OrderEntity exists in orders table
        Optional<OrderEntity> orderOpt = orderRepository.findByOrderId(orderId);
        assertThat(orderOpt).isPresent();
        OrderEntity order = orderOpt.get();
        assertThat(order.getOrderId()).isEqualTo(orderId);
        assertThat(order.getCustomerId()).isEqualTo(customerId);
        assertThat(order.getTotalAmount()).isEqualByComparingTo(totalAmount);
        assertThat(order.getItemCount()).isEqualTo(itemCount);
        assertThat(order.getCreatedAt()).isNotNull();

        // Verify processed_events record is COMPLETED
        Optional<ProcessedEventEntity> eventOpt = processedEventRepository.findByEventId(eventId);
        assertThat(eventOpt).isPresent();
        assertThat(eventOpt.get().getStatus()).isEqualTo(EventStatus.COMPLETED);
    }

    @Test
    @DisplayName("Test 4 â€” Order Duplicate: Duplicate ORDER_CREATED skips execution, preserving exactly-once order")
    void testOrderDuplicateSkipsExecution() {
        String eventId = "EVT-ORD-DUP-004";
        String orderId = "ORD-DUP-4001";

        EventEnvelope envelope = EventEnvelope.of(eventId, "ORDER_CREATED", Map.of(
                "orderId", orderId,
                "customerId", "CUST-3344",
                "totalAmount", 99.00,
                "itemCount", 1
        ));

        // First delivery: processes successfully
        ProcessingResult firstResult = idempotencyEngine.process(envelope);
        assertThat(firstResult).isEqualTo(ProcessingResult.SUCCESS);
        assertThat(orderRepository.count()).isEqualTo(1);

        // Second delivery (duplicate): returns SKIPPED_DUPLICATE
        ProcessingResult secondResult = idempotencyEngine.process(envelope);
        assertThat(secondResult).isEqualTo(ProcessingResult.SKIPPED_DUPLICATE);

        // Invariant: Exactly 1 order record in database
        assertThat(orderRepository.count()).isEqualTo(1);
        Optional<OrderEntity> orderOpt = orderRepository.findByOrderId(orderId);
        assertThat(orderOpt).isPresent();
    }

    @Test
    @DisplayName("Test 5 â€” Business Failure Rollback: Invalid payload rolls back business mutation, marks FAILED")
    void testBusinessFailureRollback() {
        String eventId = "EVT-FAIL-PAY-005";
        // Negative amount violates business rule
        EventEnvelope invalidEnvelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", "PAY-FAIL-501",
                "userId", "USER-FAIL",
                "amount", -50.0,
                "currency", "USD"
        ));

        assertThatThrownBy(() -> idempotencyEngine.process(invalidEnvelope))
                .isInstanceOf(InvalidEventPayloadException.class)
                .hasMessageContaining("must be positive");

        // Atomicity check: NO record in payments table
        assertThat(paymentRepository.count()).isEqualTo(0);

        // Status check: processed_events marked FAILED with error message
        ProcessedEventEntity event = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(event.getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(event.getErrorMessage()).contains("must be positive");
    }

    @Test
    @DisplayName("Test 6 â€” Transactional Atomicity on Fencing Token Mismatch: business mutation rolled back")
    void testTransactionalAtomicityOnTokenMismatchRollback() {
        String eventId = "EVT-FENCE-ROLLBACK-006";
        String paymentId = "PAY-FENCE-601";

        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-FENCE",
                "amount", 100.0,
                "currency", "USD"
        ));

        // Simulate a state where event is NOT in PROCESSING with invalid token "WRONG-TOKEN"
        // (or event does not exist in PROCESSING state for that token)
        assertThatThrownBy(() -> transactionalBusinessExecutor.executeAndComplete(envelope, "INVALID-TOKEN-999"))
                .isInstanceOf(StaleProcessingLeaseException.class);

        // Invariant: Because fencing check failed inside the same transaction,
        // the paymentRepository.save() MUST have rolled back completely!
        assertThat(paymentRepository.findByPaymentId(paymentId)).isEmpty();
        assertThat(paymentRepository.count()).isEqualTo(0);
    }

    @Test
    @DisplayName("Test 7 â€” Concurrent Duplicate Processing: Concurrent competing deliveries produce exactly 1 business mutation")
    void testConcurrentDuplicateProcessing() throws Exception {
        String eventId = "EVT-CONC-BIZ-007";
        String paymentId = "PAY-CONC-701";

        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-CONC",
                "amount", 250.0,
                "currency", "USD"
        ));

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        List<Future<ProcessingResult>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                startLatch.await();
                try {
                    return idempotencyEngine.process(envelope);
                } finally {
                    doneLatch.countDown();
                }
            }));
        }

        // Unleash all threads concurrently
        startLatch.countDown();
        boolean finished = doneLatch.await(30, TimeUnit.SECONDS);
        assertThat(finished).isTrue();

        int successCount = 0;
        int inProgressCount = 0;
        int duplicateCount = 0;
        for (Future<ProcessingResult> future : futures) {
            try {
                ProcessingResult res = future.get();
                if (res == ProcessingResult.SUCCESS) {
                    successCount++;
                } else if (res == ProcessingResult.SKIPPED_DUPLICATE) {
                    duplicateCount++;
                }
            } catch (ExecutionException e) {
                if (e.getCause() instanceof EventProcessingInProgressException) {
                    inProgressCount++;
                } else {
                    throw e;
                }
            }
        }

        // Exactly one thread successfully committed the business logic
        assertThat(successCount).isEqualTo(1);
        assertThat(inProgressCount + duplicateCount).isEqualTo(threadCount - 1);

        // Authoritative business verification: Exactly 1 payment record exists in PostgreSQL
        assertThat(paymentRepository.count()).isEqualTo(1);
        Optional<PaymentEntity> paymentOpt = paymentRepository.findByPaymentId(paymentId);
        assertThat(paymentOpt).isPresent();
        assertThat(paymentOpt.get().getPaymentId()).isEqualTo(paymentId);

        // Processed event is COMPLETED
        ProcessedEventEntity event = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(event.getStatus()).isEqualTo(EventStatus.COMPLETED);

        executor.shutdown();
    }
}