package com.example.eventprocessor.concurrency;

import com.example.eventprocessor.AbstractPostgresIntegrationTest;
import com.example.eventprocessor.cache.IdempotencyCacheService;
import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.entity.OrderEntity;
import com.example.eventprocessor.entity.PaymentEntity;
import com.example.eventprocessor.entity.ProcessedEventEntity;
import com.example.eventprocessor.exception.EventProcessingInProgressException;
import com.example.eventprocessor.exception.InvalidEventPayloadException;
import com.example.eventprocessor.exception.StaleProcessingLeaseException;
import com.example.eventprocessor.idempotency.IdempotencyEngine;
import com.example.eventprocessor.idempotency.IdempotencyService;
import com.example.eventprocessor.idempotency.ProcessingResult;
import com.example.eventprocessor.idempotency.ReservationResult;
import com.example.eventprocessor.kafka.consumer.DefaultEventProcessingDelegate;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.processing.TransactionalBusinessExecutor;
import com.example.eventprocessor.repository.OrderRepository;
import com.example.eventprocessor.repository.PaymentRepository;
import com.example.eventprocessor.repository.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResilienceAndFencingIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private IdempotencyEngine idempotencyEngine;

    @Autowired
    private IdempotencyService idempotencyService;

    @Autowired
    private TransactionalBusinessExecutor transactionalBusinessExecutor;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private IdempotencyCacheService idempotencyCacheService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

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
        var keys = stringRedisTemplate.keys("event-processing:*");
        if (keys != null && !keys.isEmpty()) {
            stringRedisTemplate.delete(keys);
        }
    }

    @Test
    @DisplayName("SCENARIO 7 — Lease Expiration + Fencing Under Concurrency: stale Worker A is fenced out after Worker B reclaims and commits")
    void testLeaseExpirationAndFencingTokenProtection() {
        String eventId = "EVT-FENCE-CONC-007";
        String paymentId = "PAY-FENCE-701";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-FENCE-7",
                "amount", 555.0,
                "currency", "USD"
        ));

        // 1. Worker A reserves event in PROCESSING and gets TOKEN-A
        ReservationResult resA = idempotencyService.tryReserve(envelope);
        assertThat(resA.isReserved()).isTrue();
        String tokenA = resA.processingToken();
        assertThat(tokenA).isNotBlank();

        // 2. Lease expires deterministically (age updated_at by 65s > 60s lease)
        Instant expiredTime = Instant.now().minus(Duration.ofSeconds(65));
        jdbcTemplate.update("UPDATE processed_events SET updated_at = ? WHERE event_id = ?",
                Timestamp.from(expiredTime), eventId);

        // 3. Worker B reclaims the stale lease and gets TOKEN-B
        ReservationResult resB = idempotencyService.tryReserve(envelope);
        assertThat(resB.isReserved()).isTrue();
        String tokenB = resB.processingToken();
        assertThat(tokenB).isNotBlank().isNotEqualTo(tokenA);

        // 4. Worker B executes business logic and atomically commits status COMPLETED
        transactionalBusinessExecutor.executeAndComplete(envelope, tokenB);
        idempotencyCacheService.markCompleted(eventId);

        // Verify Worker B's mutation committed
        Optional<PaymentEntity> paymentOpt = paymentRepository.findByPaymentId(paymentId);
        assertThat(paymentOpt).isPresent();
        ProcessedEventEntity eventEntity = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(eventEntity.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(eventEntity.getProcessingToken()).isEqualTo(tokenB);

        // 5. Stale Worker A cannot complete or fail the event via idempotencyService using stale TOKEN-A
        boolean workerACompleted = idempotencyService.markCompleted(eventId, tokenA);
        assertThat(workerACompleted).isFalse();

        boolean workerAFailed = idempotencyService.markFailed(eventId, tokenA, "Worker A timeout");
        assertThat(workerAFailed).isFalse();

        // 6. Stale Worker A attempts executeAndComplete using stale TOKEN-A
        // Worker A attempts to persist payment PAY-FENCE-A, but fencing check must reject and roll it back
        EventEnvelope envelopeWorkerA = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", "PAY-FENCE-STALE-A",
                "userId", "USER-FENCE-STALE-A",
                "amount", 111.0,
                "currency", "USD"
        ));

        assertThatThrownBy(() -> transactionalBusinessExecutor.executeAndComplete(envelopeWorkerA, tokenA))
                .isInstanceOf(StaleProcessingLeaseException.class)
                .hasMessageContaining(tokenA);

        // 7. Verification of invariants:
        // - Worker A's attempted payment was completely rolled back
        assertThat(paymentRepository.findByPaymentId("PAY-FENCE-STALE-A")).isEmpty();
        // - Exactly 1 payment in PostgreSQL (Worker B's payment)
        assertThat(paymentRepository.count()).isEqualTo(1);
        Optional<PaymentEntity> paymentB = paymentRepository.findByPaymentId(paymentId);
        assertThat(paymentB).isPresent();
        // - Token in database remains TOKEN-B
        ProcessedEventEntity afterA = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(afterA.getProcessingToken()).isEqualTo(tokenB);
        assertThat(afterA.getStatus()).isEqualTo(EventStatus.COMPLETED);
        // - Redis cache contains COMPLETED
        assertThat(idempotencyCacheService.isCompleted(eventId)).isTrue();
    }

    @Test
    @DisplayName("SCENARIO 8 — Real Redis Outage Under Concurrent Processing: 25 workers fail open to PostgreSQL, restore resumes caching")
    void testRealRedisOutageUnderConcurrentProcessing() throws Exception {
        int workerCount = 25;
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(workerCount);

        List<Future<ProcessingResult>> futures = new ArrayList<>();
        List<String> eventIds = new ArrayList<>();
        List<String> paymentIds = new ArrayList<>();

        for (int i = 1; i <= workerCount; i++) {
            String eventId = "EVT-REDIS-OUTAGE-" + i;
            String paymentId = "PAY-REDIS-OUTAGE-" + i;
            eventIds.add(eventId);
            paymentIds.add(paymentId);

            EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                    "paymentId", paymentId,
                    "userId", "USER-OUTAGE-" + i,
                    "amount", 100.0 + i,
                    "currency", "USD"
            ));

            futures.add(executor.submit(() -> {
                startLatch.await();
                try {
                    return idempotencyEngine.process(envelope);
                } finally {
                    doneLatch.countDown();
                }
            }));
        }

        try {
            // Pause the real Redis Testcontainer to simulate real infrastructure outage
            AbstractPostgresIntegrationTest.pauseRedis();

            // Release all 25 workers simultaneously while Redis is paused/unreachable
            startLatch.countDown();
            boolean completed = doneLatch.await(45, TimeUnit.SECONDS);
            assertThat(completed).isTrue();

            // Verify all 25 operations succeeded via PostgreSQL fail-open path
            for (Future<ProcessingResult> future : futures) {
                ProcessingResult result = future.get();
                assertThat(result).isEqualTo(ProcessingResult.SUCCESS);
            }

            // Exactly 25 business records in PostgreSQL
            assertThat(paymentRepository.count()).isEqualTo(workerCount);

            // Exactly 25 completed processed_events in PostgreSQL
            assertThat(processedEventRepository.count()).isEqualTo(workerCount);
            for (String eventId : eventIds) {
                Optional<ProcessedEventEntity> entityOpt = processedEventRepository.findByEventId(eventId);
                assertThat(entityOpt).isPresent();
                assertThat(entityOpt.get().getStatus()).isEqualTo(EventStatus.COMPLETED);
            }

        } finally {
            // Restore Redis container
            AbstractPostgresIntegrationTest.unpauseRedis();
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }

        // Verify that after unpause, Redis caching resumes seamlessly
        String newEventId = "EVT-REDIS-RESTORED-001";
        String newPaymentId = "PAY-REDIS-RESTORED-001";
        EventEnvelope restoredEnvelope = EventEnvelope.of(newEventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", newPaymentId,
                "userId", "USER-RESTORED",
                "amount", 999.99,
                "currency", "USD"
        ));

        ProcessingResult resultAfterRestore = idempotencyEngine.process(restoredEnvelope);
        assertThat(resultAfterRestore).isEqualTo(ProcessingResult.SUCCESS);

        // Redis cache is populated
        assertThat(idempotencyCacheService.isCompleted(newEventId)).isTrue();

        // Subsequent duplicate hits Redis fast-path
        ProcessingResult duplicateResult = idempotencyEngine.process(restoredEnvelope);
        assertThat(duplicateResult).isEqualTo(ProcessingResult.SKIPPED_DUPLICATE);
    }

    @Test
    @DisplayName("SCENARIO 9 — Concurrent Mixed Workload: 40 mixed operations (payments, orders, duplicates, fatal, transient retry)")
    void testConcurrentMixedWorkload() throws Exception {
        int totalWorkers = 40;
        ExecutorService executor = Executors.newFixedThreadPool(totalWorkers);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalWorkers);

        // Map to track transient attempts per eventId
        Map<String, AtomicInteger> transientAttempts = new ConcurrentHashMap<>();

        defaultEventProcessingDelegate.setCustomHandler(env -> {
            String eventId = env.eventId();
            if (eventId.startsWith("EVT-MIX-TRANS-")) {
                AtomicInteger attempts = transientAttempts.computeIfAbsent(eventId, k -> new AtomicInteger(0));
                if (attempts.incrementAndGet() == 1) {
                    throw new CannotAcquireLockException("Simulated transient lock contention on first attempt");
                }
            }
        });

        List<Future<?>> futures = new ArrayList<>();

        // 1. 10 Valid Payments (EVT-MIX-PAY-1 to 10)
        for (int i = 1; i <= 10; i++) {
            EventEnvelope env = EventEnvelope.of("EVT-MIX-PAY-" + i, "PAYMENT_COMPLETED", Map.of(
                    "paymentId", "PAY-MIX-" + i,
                    "userId", "USER-MIX-" + i,
                    "amount", 50.0 + i,
                    "currency", "USD"
            ));
            futures.add(executor.submit(() -> runTask(startLatch, doneLatch, env)));
        }

        // 2. 10 Valid Orders (EVT-MIX-ORD-1 to 10)
        for (int i = 1; i <= 10; i++) {
            EventEnvelope env = EventEnvelope.of("EVT-MIX-ORD-" + i, "ORDER_CREATED", Map.of(
                    "orderId", "ORD-MIX-" + i,
                    "customerId", "CUST-MIX-" + i,
                    "totalAmount", 100.0 + i,
                    "itemCount", i
            ));
            futures.add(executor.submit(() -> runTask(startLatch, doneLatch, env)));
        }

        // 3. 10 Concurrent Duplicates (5 for payment 1..5, 5 for order 1..5)
        for (int i = 1; i <= 5; i++) {
            EventEnvelope env = EventEnvelope.of("EVT-MIX-PAY-" + i, "PAYMENT_COMPLETED", Map.of(
                    "paymentId", "PAY-MIX-" + i,
                    "userId", "USER-MIX-" + i,
                    "amount", 50.0 + i,
                    "currency", "USD"
            ));
            futures.add(executor.submit(() -> runTask(startLatch, doneLatch, env)));
        }
        for (int i = 1; i <= 5; i++) {
            EventEnvelope env = EventEnvelope.of("EVT-MIX-ORD-" + i, "ORDER_CREATED", Map.of(
                    "orderId", "ORD-MIX-" + i,
                    "customerId", "CUST-MIX-" + i,
                    "totalAmount", 100.0 + i,
                    "itemCount", i
            ));
            futures.add(executor.submit(() -> runTask(startLatch, doneLatch, env)));
        }

        // 4. 5 Fatal Invalid Events (negative amount)
        for (int i = 1; i <= 5; i++) {
            EventEnvelope env = EventEnvelope.of("EVT-MIX-FATAL-" + i, "PAYMENT_COMPLETED", Map.of(
                    "paymentId", "PAY-MIX-FATAL-" + i,
                    "userId", "USER-FATAL-" + i,
                    "amount", -10.0 * i,
                    "currency", "USD"
            ));
            futures.add(executor.submit(() -> runTask(startLatch, doneLatch, env)));
        }

        // 5. 5 Transient Events (simulated lock, retries once)
        for (int i = 1; i <= 5; i++) {
            EventEnvelope env = EventEnvelope.of("EVT-MIX-TRANS-" + i, "PAYMENT_COMPLETED", Map.of(
                    "paymentId", "PAY-MIX-TRANS-" + i,
                    "userId", "USER-TRANS-" + i,
                    "amount", 200.0 + i,
                    "currency", "USD"
            ));
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    // Attempt 1 fails with transient error, prepare for retry, then retry succeeds
                    try {
                        idempotencyEngine.process(env);
                    } catch (CannotAcquireLockException e) {
                        // Retry attempt 2 succeeds
                        idempotencyEngine.process(env);
                    }
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            }));
        }

        // Unleash all 40 workers concurrently
        startLatch.countDown();
        boolean completed = doneLatch.await(45, TimeUnit.SECONDS);
        assertThat(completed).isTrue();

        // Verification:
        // Exactly 10 payments for EVT-MIX-PAY-1..10 + 5 for EVT-MIX-TRANS-1..5 = 15 total payments
        assertThat(paymentRepository.count()).isEqualTo(15);

        // Exactly 10 orders for EVT-MIX-ORD-1..10
        assertThat(orderRepository.count()).isEqualTo(10);

        // Verify each valid payment exists exactly once
        for (int i = 1; i <= 10; i++) {
            assertThat(paymentRepository.findByPaymentId("PAY-MIX-" + i)).isPresent();
        }

        // Verify each valid order exists exactly once
        for (int i = 1; i <= 10; i++) {
            assertThat(orderRepository.findByOrderId("ORD-MIX-" + i)).isPresent();
        }

        // Verify fatal events marked FAILED with zero payments created
        for (int i = 1; i <= 5; i++) {
            String fatalEventId = "EVT-MIX-FATAL-" + i;
            Optional<ProcessedEventEntity> fatalOpt = processedEventRepository.findByEventId(fatalEventId);
            assertThat(fatalOpt).isPresent();
            assertThat(fatalOpt.get().getStatus()).isEqualTo(EventStatus.FAILED);
            assertThat(paymentRepository.findByPaymentId("PAY-MIX-FATAL-" + i)).isEmpty();
        }

        // Verify transient events ended COMPLETED
        for (int i = 1; i <= 5; i++) {
            String transEventId = "EVT-MIX-TRANS-" + i;
            Optional<ProcessedEventEntity> transOpt = processedEventRepository.findByEventId(transEventId);
            assertThat(transOpt).isPresent();
            assertThat(transOpt.get().getStatus()).isEqualTo(EventStatus.COMPLETED);
        }

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    private void runTask(CountDownLatch startLatch, CountDownLatch doneLatch, EventEnvelope env) {
        try {
            startLatch.await();
            idempotencyEngine.process(env);
        } catch (Exception ignored) {
        } finally {
            doneLatch.countDown();
        }
    }

    @Test
    @DisplayName("SCENARIO 10 — Concurrent Transaction Rollback: failed transactions leave zero partial records, successful commit")
    void testConcurrentTransactionRollback() throws Exception {
        int pairCount = 10;
        int totalWorkers = pairCount * 2;
        ExecutorService executor = Executors.newFixedThreadPool(totalWorkers);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(totalWorkers);

        List<String> validEventIds = new ArrayList<>();
        List<String> validPaymentIds = new ArrayList<>();
        List<String> invalidEventIds = new ArrayList<>();
        List<String> invalidPaymentIds = new ArrayList<>();

        for (int i = 1; i <= pairCount; i++) {
            String validEvt = "EVT-ROLL-VAL-" + i;
            String validPay = "PAY-ROLL-VAL-" + i;
            validEventIds.add(validEvt);
            validPaymentIds.add(validPay);

            EventEnvelope validEnvelope = EventEnvelope.of(validEvt, "PAYMENT_COMPLETED", Map.of(
                    "paymentId", validPay,
                    "userId", "USER-VAL-" + i,
                    "amount", 100.0 * i,
                    "currency", "USD"
            ));
            executor.submit(() -> runTask(startLatch, doneLatch, validEnvelope));

            String invalidEvt = "EVT-ROLL-INVAL-" + i;
            String invalidPay = "PAY-ROLL-INVAL-" + i;
            invalidEventIds.add(invalidEvt);
            invalidPaymentIds.add(invalidPay);

            EventEnvelope invalidEnvelope = EventEnvelope.of(invalidEvt, "PAYMENT_COMPLETED", Map.of(
                    "paymentId", invalidPay,
                    "userId", "USER-INVAL-" + i,
                    "amount", -50.0 * i, // Negative amount forces rollback
                    "currency", "USD"
            ));
            executor.submit(() -> runTask(startLatch, doneLatch, invalidEnvelope));
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(45, TimeUnit.SECONDS);
        assertThat(completed).isTrue();

        // 1. Exactly 10 payments must exist in PostgreSQL (the valid ones)
        assertThat(paymentRepository.count()).isEqualTo(pairCount);

        for (String validPay : validPaymentIds) {
            assertThat(paymentRepository.findByPaymentId(validPay)).isPresent();
        }

        for (String validEvt : validEventIds) {
            ProcessedEventEntity entity = processedEventRepository.findByEventId(validEvt).orElseThrow();
            assertThat(entity.getStatus()).isEqualTo(EventStatus.COMPLETED);
        }

        // 2. The 10 invalid transactions must have rolled back completely: ZERO rows in payments table
        for (String invalidPay : invalidPaymentIds) {
            assertThat(paymentRepository.findByPaymentId(invalidPay)).isEmpty();
        }

        for (String invalidEvt : invalidEventIds) {
            ProcessedEventEntity entity = processedEventRepository.findByEventId(invalidEvt).orElseThrow();
            assertThat(entity.getStatus()).isEqualTo(EventStatus.FAILED);
            assertThat(entity.getErrorMessage()).contains("must be positive");
        }

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
}
