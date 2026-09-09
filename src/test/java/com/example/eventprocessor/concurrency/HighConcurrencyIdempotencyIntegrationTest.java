package com.example.eventprocessor.concurrency;

import com.example.eventprocessor.AbstractPostgresIntegrationTest;
import com.example.eventprocessor.cache.IdempotencyCacheService;
import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.entity.PaymentEntity;
import com.example.eventprocessor.entity.ProcessedEventEntity;
import com.example.eventprocessor.exception.EventProcessingInProgressException;
import com.example.eventprocessor.idempotency.IdempotencyEngine;
import com.example.eventprocessor.idempotency.ProcessingResult;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.repository.PaymentRepository;
import com.example.eventprocessor.repository.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class HighConcurrencyIdempotencyIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private IdempotencyEngine idempotencyEngine;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private IdempotencyCacheService idempotencyCacheService;

    @BeforeEach
    void setUp() {
        processedEventRepository.deleteAll();
        paymentRepository.deleteAll();
        var keys = stringRedisTemplate.keys("event-processing:*");
        if (keys != null && !keys.isEmpty()) {
            stringRedisTemplate.delete(keys);
        }
    }

    @Test
    @DisplayName("SCENARIO 1 — 50+ Concurrent Distinct Events: all 50 commit successfully with atomic COMPLETED state and Redis cache")
    void testFiftyConcurrentDistinctEvents() throws Exception {
        int eventCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(eventCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(eventCount);

        List<Future<ProcessingResult>> futures = new ArrayList<>();
        for (int i = 1; i <= eventCount; i++) {
            String eventId = "EVT-CONC-DIST-" + i;
            String paymentId = "PAY-CONC-DIST-" + i;
            EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                    "paymentId", paymentId,
                    "userId", "USER-CONC-" + i,
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

        // Release all 50 workers simultaneously
        startLatch.countDown();
        boolean completed = doneLatch.await(45, TimeUnit.SECONDS);
        assertThat(completed).isTrue();

        // Verify all 50 futures returned SUCCESS
        int successCount = 0;
        for (Future<ProcessingResult> future : futures) {
            ProcessingResult result = future.get();
            if (result == ProcessingResult.SUCCESS) {
                successCount++;
            }
        }
        assertThat(successCount).isEqualTo(eventCount);

        // Verify exactly 50 business mutations exist in PostgreSQL
        assertThat(paymentRepository.count()).isEqualTo(eventCount);

        // Verify all 50 processed_events records exist and are COMPLETED
        List<ProcessedEventEntity> events = processedEventRepository.findAll();
        assertThat(events).hasSize(eventCount);
        for (ProcessedEventEntity event : events) {
            assertThat(event.getStatus()).isEqualTo(EventStatus.COMPLETED);
            assertThat(event.getProcessedAt()).isNotNull();
            assertThat(event.getProcessingToken()).isNotNull();
        }

        // Verify all 50 have Redis completed cache entries
        for (int i = 1; i <= eventCount; i++) {
            String key = "event-processing:EVT-CONC-DIST-" + i;
            String cachedValue = stringRedisTemplate.opsForValue().get(key);
            assertThat(cachedValue).isEqualTo("COMPLETED");
        }

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @DisplayName("SCENARIO 2 — 50+ Concurrent Duplicates for New Uncached Event: exactly 1 wins, 49 handled safely, Redis populated")
    void testFiftyConcurrentDuplicatesNewUncachedEvent() throws Exception {
        String eventId = "EVT-CONC-STAMPEDE-001";
        String paymentId = "PAY-CONC-STAMPEDE-001";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-STAMPEDE",
                "amount", 999.00,
                "currency", "USD"
        ));

        // Ensure Redis does not contain the event initially
        assertThat(idempotencyCacheService.isCompleted(eventId)).isFalse();

        int workerCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(workerCount);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger duplicateCount = new AtomicInteger(0);
        AtomicInteger inProgressCount = new AtomicInteger(0);
        AtomicInteger otherErrorCount = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    ProcessingResult result = idempotencyEngine.process(envelope);
                    if (result == ProcessingResult.SUCCESS) {
                        successCount.incrementAndGet();
                    } else if (result == ProcessingResult.SKIPPED_DUPLICATE) {
                        duplicateCount.incrementAndGet();
                    }
                } catch (EventProcessingInProgressException e) {
                    inProgressCount.incrementAndGet();
                } catch (Exception e) {
                    otherErrorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            }));
        }

        // Release all 50 workers simultaneously
        startLatch.countDown();
        boolean completed = doneLatch.await(45, TimeUnit.SECONDS);
        assertThat(completed).isTrue();

        // Exactly one worker must successfully commit the business logic
        assertThat(successCount.get()).isEqualTo(1);
        assertThat(otherErrorCount.get()).isEqualTo(0);
        assertThat(duplicateCount.get() + inProgressCount.get()).isEqualTo(workerCount - 1);

        // Authoritative verification: Exactly 1 payment record in PostgreSQL
        assertThat(paymentRepository.count()).isEqualTo(1);
        Optional<PaymentEntity> paymentOpt = paymentRepository.findByPaymentId(paymentId);
        assertThat(paymentOpt).isPresent();
        assertThat(paymentOpt.get().getAmount()).isEqualByComparingTo(new BigDecimal("999.00"));

        // Exactly 1 processed_events record with status COMPLETED
        Optional<ProcessedEventEntity> eventOpt = processedEventRepository.findByEventId(eventId);
        assertThat(eventOpt).isPresent();
        assertThat(eventOpt.get().getStatus()).isEqualTo(EventStatus.COMPLETED);

        // Verify Redis cache is populated
        assertThat(idempotencyCacheService.isCompleted(eventId)).isTrue();
        String cachedValue = stringRedisTemplate.opsForValue().get("event-processing:" + eventId);
        assertThat(cachedValue).isEqualTo("COMPLETED");

        // Subsequent duplicate calls hit the Redis fast-path
        for (int i = 0; i < 5; i++) {
            ProcessingResult subsequentResult = idempotencyEngine.process(envelope);
            assertThat(subsequentResult).isEqualTo(ProcessingResult.SKIPPED_DUPLICATE);
        }
        assertThat(paymentRepository.count()).isEqualTo(1);

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @DisplayName("SCENARIO 3 — 50+ Concurrent Duplicates for Already-Cached Event: all 50 skip execution via Redis fast-path")
    void testFiftyConcurrentDuplicatesAlreadyCachedEvent() throws Exception {
        String eventId = "EVT-CONC-PRECACHED-001";
        String paymentId = "PAY-CONC-PRECACHED-001";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-PRECACHED",
                "amount", 777.00,
                "currency", "USD"
        ));

        // Initial delivery commits and populates cache
        ProcessingResult firstResult = idempotencyEngine.process(envelope);
        assertThat(firstResult).isEqualTo(ProcessingResult.SUCCESS);
        assertThat(idempotencyCacheService.isCompleted(eventId)).isTrue();
        assertThat(paymentRepository.count()).isEqualTo(1);

        int workerCount = 50;
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(workerCount);

        AtomicInteger skippedCount = new AtomicInteger(0);
        AtomicInteger otherCount = new AtomicInteger(0);

        for (int i = 0; i < workerCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    ProcessingResult result = idempotencyEngine.process(envelope);
                    if (result == ProcessingResult.SKIPPED_DUPLICATE) {
                        skippedCount.incrementAndGet();
                    } else {
                        otherCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    otherCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        assertThat(completed).isTrue();

        // All 50 concurrent calls must be classified as SKIPPED_DUPLICATE
        assertThat(skippedCount.get()).isEqualTo(workerCount);
        assertThat(otherCount.get()).isEqualTo(0);

        // Invariant: No additional business mutations created in PostgreSQL
        assertThat(paymentRepository.count()).isEqualTo(1);
        assertThat(processedEventRepository.count()).isEqualTo(1);

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
}
