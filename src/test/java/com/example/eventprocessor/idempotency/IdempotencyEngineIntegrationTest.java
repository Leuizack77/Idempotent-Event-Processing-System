package com.example.eventprocessor.idempotency;

import com.example.eventprocessor.AbstractPostgresIntegrationTest;
import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.entity.ProcessedEventEntity;
import com.example.eventprocessor.exception.EventProcessingInProgressException;
import com.example.eventprocessor.kafka.consumer.DefaultEventProcessingDelegate;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.repository.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdempotencyEngineIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private IdempotencyEngine idempotencyEngine;

    @Autowired
    private IdempotencyService idempotencyService;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private com.example.eventprocessor.repository.PaymentRepository paymentRepository;

    @Autowired
    private com.example.eventprocessor.repository.OrderRepository orderRepository;

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
    @DisplayName("TEST 1 — COMPLETED duplicate: existing COMPLETED event skips delegate, returns SKIPPED_DUPLICATE, Kafka may ACK")
    void testCompletedDuplicate() {
        String eventId = "EVT-IDEMP-COMPLETED-001";
        ProcessedEventEntity preCompleted = new ProcessedEventEntity(eventId, "PAYMENT_COMPLETED", EventStatus.COMPLETED);
        preCompleted.setProcessedAt(Instant.now());
        processedEventRepository.saveAndFlush(preCompleted);

        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of("amount", 100.0));

        AtomicInteger delegateInvocations = new AtomicInteger(0);
        defaultEventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                delegateInvocations.incrementAndGet();
            }
        });

        ProcessingResult result = idempotencyEngine.process(envelope);

        // Crucial: Only COMPLETED produces SKIPPED_DUPLICATE permitting Kafka ACK
        assertThat(result).isEqualTo(ProcessingResult.SKIPPED_DUPLICATE);
        assertThat(delegateInvocations.get()).isEqualTo(0);

        Optional<ProcessedEventEntity> entityOpt = processedEventRepository.findByEventId(eventId);
        assertThat(entityOpt).isPresent();
        assertThat(entityOpt.get().getStatus()).isEqualTo(EventStatus.COMPLETED);
    }

    @Test
    @DisplayName("TEST 2 — Active PROCESSING: event in PROCESSING within lease throws EventProcessingInProgressException, no ACK")
    void testActiveProcessingThrowsException() {
        String eventId = "EVT-IDEMP-ACTIVE-002";
        // Create event with recent updated_at (active within 60s lease)
        ProcessedEventEntity activeEntity = new ProcessedEventEntity(eventId, "PAYMENT_COMPLETED", EventStatus.PROCESSING);
        processedEventRepository.saveAndFlush(activeEntity);

        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of("amount", 200.0));

        AtomicInteger delegateInvocations = new AtomicInteger(0);
        defaultEventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                delegateInvocations.incrementAndGet();
            }
        });

        // Must throw EventProcessingInProgressException (NOT return SKIPPED_DUPLICATE)
        assertThatThrownBy(() -> idempotencyEngine.process(envelope))
                .isInstanceOf(EventProcessingInProgressException.class)
                .hasMessageContaining(eventId);

        // Business delegate must not have been executed
        assertThat(delegateInvocations.get()).isEqualTo(0);

        // Database status remains PROCESSING
        ProcessedEventEntity entity = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(entity.getStatus()).isEqualTo(EventStatus.PROCESSING);
    }

    @Test
    @DisplayName("TEST 3 — Stale PROCESSING recovery: expired lease allows atomic reclaim, retry_count increment, and completion")
    void testStaleProcessingRecovery() {
        String eventId = "EVT-IDEMP-STALE-003";
        ProcessedEventEntity staleEntity = new ProcessedEventEntity(eventId, "PAYMENT_COMPLETED", EventStatus.PROCESSING);
        staleEntity.setRetryCount(0);
        processedEventRepository.saveAndFlush(staleEntity);

        // Age updated_at by 5 minutes (exceeding default 60s lease)
        Instant fiveMinutesAgo = Instant.now().minus(Duration.ofMinutes(5));
        jdbcTemplate.update("UPDATE processed_events SET updated_at = ? WHERE event_id = ?",
                Timestamp.from(fiveMinutesAgo), eventId);

        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", "PAY-STALE-003",
                "userId", "USR-3",
                "amount", 300.0,
                "currency", "USD"
        ));

        AtomicInteger delegateInvocations = new AtomicInteger(0);
        defaultEventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                delegateInvocations.incrementAndGet();
            }
        });

        ProcessingResult result = idempotencyEngine.process(envelope);

        assertThat(result).isEqualTo(ProcessingResult.SUCCESS);
        assertThat(delegateInvocations.get()).isEqualTo(1);

        ProcessedEventEntity finalEntity = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(finalEntity.getStatus()).isEqualTo(EventStatus.COMPLETED);
        // Retry count should increment from 0 to 1 during stale reclaim
        assertThat(finalEntity.getRetryCount()).isEqualTo(1);
        assertThat(finalEntity.getProcessedAt()).isNotNull();
    }

    @Test
    @DisplayName("TEST 4 — Concurrent stale reclaim: multiple threads race on 1 stale event, exactly 1 reclaims and completes")
    void testConcurrentStaleReclaim() throws Exception {
        String eventId = "EVT-IDEMP-CONCURRENT-STALE-004";
        ProcessedEventEntity staleEntity = new ProcessedEventEntity(eventId, "PAYMENT_COMPLETED", EventStatus.PROCESSING);
        staleEntity.setRetryCount(0);
        processedEventRepository.saveAndFlush(staleEntity);

        // Age updated_at by 5 minutes
        Instant fiveMinutesAgo = Instant.now().minus(Duration.ofMinutes(5));
        jdbcTemplate.update("UPDATE processed_events SET updated_at = ? WHERE event_id = ?",
                Timestamp.from(fiveMinutesAgo), eventId);

        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", "PAY-CONC-004",
                "userId", "USR-4",
                "amount", 400.0,
                "currency", "USD"
        ));

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        AtomicInteger businessInvocations = new AtomicInteger(0);
        defaultEventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                businessInvocations.incrementAndGet();
                try {
                    Thread.sleep(40);
                } catch (InterruptedException ignored) {
                }
            }
        });

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

        startLatch.countDown();
        boolean finished = doneLatch.await(30, TimeUnit.SECONDS);
        assertThat(finished).isTrue();

        // Exactly one business invocation across all competing threads
        assertThat(businessInvocations.get()).isEqualTo(1);

        int successCount = 0;
        int inProgressExceptionCount = 0;
        for (Future<ProcessingResult> future : futures) {
            try {
                ProcessingResult res = future.get();
                if (res == ProcessingResult.SUCCESS) {
                    successCount++;
                }
            } catch (ExecutionException e) {
                if (e.getCause() instanceof EventProcessingInProgressException) {
                    inProgressExceptionCount++;
                } else {
                    throw e;
                }
            }
        }

        assertThat(successCount).isEqualTo(1);
        assertThat(inProgressExceptionCount).isEqualTo(threadCount - 1);

        ProcessedEventEntity finalEntity = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(finalEntity.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(finalEntity.getRetryCount()).isEqualTo(1);

        executor.shutdown();
    }

    @Test
    @DisplayName("TEST 4 — Old owner cannot complete after reclaim: Worker A (TOKEN-A) is fenced out after Worker B reclaims with TOKEN-B")
    void testOldOwnerCannotCompleteAfterReclaim() {
        String eventId = "EVT-FENCE-COMPLETE-004";
        String tokenA = "TOKEN-A-WORKER-1";

        // 1. Worker A reserves the event in PROCESSING with tokenA
        ProcessedEventEntity entityA = new ProcessedEventEntity(eventId, "PAYMENT_COMPLETED", EventStatus.PROCESSING);
        entityA.setProcessingToken(tokenA);
        entityA.setRetryCount(0);
        processedEventRepository.saveAndFlush(entityA);

        // 2. Lease becomes stale (simulate time passing > lease duration)
        Instant fiveMinutesAgo = Instant.now().minus(Duration.ofMinutes(5));
        jdbcTemplate.update("UPDATE processed_events SET updated_at = ? WHERE event_id = ?",
                Timestamp.from(fiveMinutesAgo), eventId);

        // 3. Worker B reclaims the event
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of("amount", 100.0));
        ReservationResult reservationB = idempotencyService.tryReserve(envelope);
        assertThat(reservationB.isReserved()).isTrue();
        String tokenB = reservationB.processingToken();
        assertThat(tokenB).isNotBlank().isNotEqualTo(tokenA);

        // Verify DB currently has Worker B's token
        ProcessedEventEntity current = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(current.getProcessingToken()).isEqualTo(tokenB);
        assertThat(current.getStatus()).isEqualTo(EventStatus.PROCESSING);

        // 4. Stale Worker A finishes late and attempts markCompleted with tokenA
        boolean workerACompleted = idempotencyService.markCompleted(eventId, tokenA);
        assertThat(workerACompleted).isFalse();

        // 5. Verify Worker B's state and token are preserved, status is still PROCESSING (not completed by A)
        ProcessedEventEntity afterAttempt = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(afterAttempt.getProcessingToken()).isEqualTo(tokenB);
        assertThat(afterAttempt.getStatus()).isEqualTo(EventStatus.PROCESSING);
        assertThat(afterAttempt.getProcessedAt()).isNull();

        // 6. Worker B successfully completes with tokenB
        boolean workerBCompleted = idempotencyService.markCompleted(eventId, tokenB);
        assertThat(workerBCompleted).isTrue();

        ProcessedEventEntity finalState = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(finalState.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(finalState.getProcessingToken()).isEqualTo(tokenB);
        assertThat(finalState.getProcessedAt()).isNotNull();
    }

    @Test
    @DisplayName("TEST 5 — Old owner cannot mark FAILED after reclaim: Worker A (TOKEN-A) failure update fails and cannot corrupt Worker B (TOKEN-B)")
    void testOldOwnerCannotMarkFailedAfterReclaim() {
        String eventId = "EVT-FENCE-FAIL-005";
        String tokenA = "TOKEN-A-WORKER-1";

        // 1. Worker A reserves the event in PROCESSING with tokenA
        ProcessedEventEntity entityA = new ProcessedEventEntity(eventId, "PAYMENT_COMPLETED", EventStatus.PROCESSING);
        entityA.setProcessingToken(tokenA);
        entityA.setRetryCount(0);
        processedEventRepository.saveAndFlush(entityA);

        // 2. Lease becomes stale
        Instant fiveMinutesAgo = Instant.now().minus(Duration.ofMinutes(5));
        jdbcTemplate.update("UPDATE processed_events SET updated_at = ? WHERE event_id = ?",
                Timestamp.from(fiveMinutesAgo), eventId);

        // 3. Worker B reclaims the event
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of("amount", 200.0));
        ReservationResult reservationB = idempotencyService.tryReserve(envelope);
        assertThat(reservationB.isReserved()).isTrue();
        String tokenB = reservationB.processingToken();
        assertThat(tokenB).isNotBlank().isNotEqualTo(tokenA);

        // 4. Stale Worker A throws an exception and attempts markFailed with tokenA
        boolean workerAFailed = idempotencyService.markFailed(eventId, tokenA, "Worker A timeout failure");
        assertThat(workerAFailed).isFalse();

        // 5. Verify Worker B's state remains authoritative in PROCESSING, token is TOKEN-B, and error message is null
        ProcessedEventEntity current = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(current.getProcessingToken()).isEqualTo(tokenB);
        assertThat(current.getStatus()).isEqualTo(EventStatus.PROCESSING);
        assertThat(current.getErrorMessage()).isNull();

        // 6. Worker B can finish cleanly
        boolean workerBCompleted = idempotencyService.markCompleted(eventId, tokenB);
        assertThat(workerBCompleted).isTrue();

        ProcessedEventEntity finalState = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(finalState.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(finalState.getProcessingToken()).isEqualTo(tokenB);
    }

    @Test
    @DisplayName("TEST 7 — Crash/recovery semantics: worker crashes in PROCESSING, after lease expires recovery succeeds")
    void testCrashAndLeaseRecovery() {
        String eventId = "EVT-IDEMP-CRASH-005";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", "PAY-CRASH-005",
                "userId", "USR-5",
                "amount", 500.0,
                "currency", "USD"
        ));

        // 1. First attempt simulates worker starting, reserving, and crashing before markCompleted()
        ProcessedEventEntity crashedEntity = new ProcessedEventEntity(eventId, "PAYMENT_COMPLETED", EventStatus.PROCESSING);
        crashedEntity.setRetryCount(0);
        processedEventRepository.saveAndFlush(crashedEntity);

        // While lease is active, recovery attempt fails with EventProcessingInProgressException
        assertThatThrownBy(() -> idempotencyEngine.process(envelope))
                .isInstanceOf(EventProcessingInProgressException.class);

        // 2. Simulate time passing: lease expires (advance updated_at by 5 minutes)
        Instant fiveMinutesAgo = Instant.now().minus(Duration.ofMinutes(5));
        jdbcTemplate.update("UPDATE processed_events SET updated_at = ? WHERE event_id = ?",
                Timestamp.from(fiveMinutesAgo), eventId);

        // 3. Subsequent delivery now safely reclaims the orphaned event
        AtomicInteger delegateInvocations = new AtomicInteger(0);
        defaultEventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                delegateInvocations.incrementAndGet();
            }
        });

        ProcessingResult result = idempotencyEngine.process(envelope);
        assertThat(result).isEqualTo(ProcessingResult.SUCCESS);
        assertThat(delegateInvocations.get()).isEqualTo(1);

        ProcessedEventEntity recoveredEntity = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(recoveredEntity.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(recoveredEntity.getRetryCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("TEST 8 — Business failure: exception transitions to FAILED, error message recorded, remains eligible for redelivery")
    void testBusinessFailureTransitionsToFailed() {
        String eventId = "EVT-IDEMP-FAIL-006";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", "PAY-FAIL-006",
                "userId", "USR-6",
                "amount", 600.0,
                "currency", "USD"
        ));

        String errorMessage = "Simulated business explosion";
        defaultEventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                throw new RuntimeException(errorMessage);
            }
        });

        assertThatThrownBy(() -> idempotencyEngine.process(envelope))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(errorMessage);

        ProcessedEventEntity entity = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(entity.getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(entity.getErrorMessage()).contains(errorMessage);
        assertThat(entity.getProcessedAt()).isNull();

        // Verify that on subsequent delivery, FAILED event is eligible for re-processing
        defaultEventProcessingDelegate.reset();
        AtomicInteger retryInvocations = new AtomicInteger(0);
        defaultEventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                retryInvocations.incrementAndGet();
            }
        });

        ProcessingResult retryResult = idempotencyEngine.process(envelope);
        assertThat(retryResult).isEqualTo(ProcessingResult.SUCCESS);
        assertThat(retryInvocations.get()).isEqualTo(1);

        ProcessedEventEntity finalEntity = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(finalEntity.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(finalEntity.getRetryCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("TEST 9 — 50-thread concurrency test: exactly 1 wins reservation, 49 active competitors throw in-progress exception without false ACK")
    void testFiftyThreadConcurrency() throws Exception {
        String eventId = "EVT-CONCURRENT-50-007";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", "PAY-50-007",
                "userId", "USR-7",
                "amount", 700.0,
                "currency", "USD"
        ));

        int threadCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        AtomicInteger businessInvocations = new AtomicInteger(0);
        defaultEventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                businessInvocations.incrementAndGet();
                try {
                    Thread.sleep(300);
                } catch (InterruptedException ignored) {
                }
            }
        });

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

        // Release 50 threads simultaneously against PostgreSQL
        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        assertThat(completed).isTrue();

        // Exactly one business invocation across all 50 threads
        assertThat(businessInvocations.get()).isEqualTo(1);

        int successCount = 0;
        int inProgressExceptionCount = 0;
        int skippedDuplicateCount = 0;
        for (Future<ProcessingResult> future : futures) {
            try {
                ProcessingResult res = future.get();
                if (res == ProcessingResult.SUCCESS) {
                    successCount++;
                } else if (res == ProcessingResult.SKIPPED_DUPLICATE) {
                    skippedDuplicateCount++;
                }
            } catch (ExecutionException e) {
                if (e.getCause() instanceof EventProcessingInProgressException) {
                    inProgressExceptionCount++;
                } else {
                    throw e;
                }
            }
        }

        // Exactly 1 winner, all 49 competitors safely handled without duplicate execution
        assertThat(successCount).isEqualTo(1);
        assertThat(inProgressExceptionCount + skippedDuplicateCount).isEqualTo(threadCount - 1);
        assertThat(inProgressExceptionCount).isGreaterThan(0);

        // Verify exactly one row in PostgreSQL with status COMPLETED
        Optional<ProcessedEventEntity> entityOpt = processedEventRepository.findByEventId(eventId);
        assertThat(entityOpt).isPresent();
        assertThat(entityOpt.get().getStatus()).isEqualTo(EventStatus.COMPLETED);

        executor.shutdown();
    }

    @Test
    @DisplayName("TEST 10 — SUBMITTED to COMPLETED: REST-ingested event in SUBMITTED state transitions to COMPLETED")
    void testRestIngestedEventTransitionsToCompleted() {
        String eventId = "EVT-IDEMP-SUBMITTED-008";
        ProcessedEventEntity submittedEntity = new ProcessedEventEntity(eventId, "ORDER_CREATED", EventStatus.SUBMITTED);
        processedEventRepository.saveAndFlush(submittedEntity);

        EventEnvelope envelope = EventEnvelope.of(eventId, "ORDER_CREATED", Map.of(
                "orderId", "ORD-SUB-8",
                "customerId", "CUST-8",
                "totalAmount", 120.0,
                "itemCount", 2
        ));

        AtomicInteger delegateInvocations = new AtomicInteger(0);
        defaultEventProcessingDelegate.setCustomHandler(env -> {
            if (eventId.equals(env.eventId())) {
                delegateInvocations.incrementAndGet();
            }
        });

        ProcessingResult result = idempotencyEngine.process(envelope);

        assertThat(result).isEqualTo(ProcessingResult.SUCCESS);
        assertThat(delegateInvocations.get()).isEqualTo(1);

        ProcessedEventEntity finalEntity = processedEventRepository.findByEventId(eventId).orElseThrow();
        assertThat(finalEntity.getStatus()).isEqualTo(EventStatus.COMPLETED);
        assertThat(finalEntity.getProcessedAt()).isNotNull();
    }
}
