package com.example.eventprocessor.resilience;

import com.example.eventprocessor.AbstractPostgresIntegrationTest;
import com.example.eventprocessor.cache.IdempotencyCacheService;
import com.example.eventprocessor.cache.RedisIdempotencyCacheService;
import com.example.eventprocessor.config.IdempotencyProperties;
import com.example.eventprocessor.entity.EventStatus;
import com.example.eventprocessor.entity.PaymentEntity;
import com.example.eventprocessor.entity.ProcessedEventEntity;
import com.example.eventprocessor.exception.InvalidEventPayloadException;
import com.example.eventprocessor.idempotency.IdempotencyEngine;
import com.example.eventprocessor.idempotency.IdempotencyEngineImpl;
import com.example.eventprocessor.idempotency.IdempotencyService;
import com.example.eventprocessor.idempotency.ProcessingResult;
import com.example.eventprocessor.kafka.consumer.KafkaRetryClassifier;
import com.example.eventprocessor.kafka.model.EventEnvelope;
import com.example.eventprocessor.processing.TransactionalBusinessExecutor;
import com.example.eventprocessor.repository.PaymentRepository;
import com.example.eventprocessor.repository.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RedisIdempotencyOptimizationIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private IdempotencyEngine idempotencyEngine;

    @Autowired
    private IdempotencyService idempotencyService;

    @Autowired
    private TransactionalBusinessExecutor transactionalBusinessExecutor;

    @Autowired
    private KafkaRetryClassifier retryClassifier;

    @Autowired
    private IdempotencyCacheService idempotencyCacheService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private IdempotencyProperties idempotencyProperties;

    @BeforeEach
    void setUp() {
        processedEventRepository.deleteAll();
        paymentRepository.deleteAll();
        // Clean all keys with prefix event-processing:
        var keys = stringRedisTemplate.keys("event-processing:*");
        if (keys != null && !keys.isEmpty()) {
            stringRedisTemplate.delete(keys);
        }
    }

    @Test
    @DisplayName("SCENARIO A — Redis cache HIT for a completed event: bypasses PostgreSQL reservation & skips duplicate")
    void testRedisCacheHitBypassesPostgresReservation() {
        String eventId = "EVT-REDIS-HIT-001";
        String paymentId = "PAY-REDIS-001";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-1",
                "amount", 100.0,
                "currency", "USD"
        ));

        // Pre-populate Redis cache directly
        idempotencyCacheService.markCompleted(eventId);
        assertThat(idempotencyCacheService.isCompleted(eventId)).isTrue();

        // Process event through idempotency engine
        ProcessingResult result = idempotencyEngine.process(envelope);

        // Verify fast-path short-circuit
        assertThat(result).isEqualTo(ProcessingResult.SKIPPED_DUPLICATE);

        // Verify PostgreSQL reservation was completely bypassed
        Optional<ProcessedEventEntity> eventOpt = processedEventRepository.findByEventId(eventId);
        assertThat(eventOpt).isEmpty();

        // Verify business execution was bypassed
        Optional<PaymentEntity> paymentOpt = paymentRepository.findByPaymentId(paymentId);
        assertThat(paymentOpt).isEmpty();
    }

    @Test
    @DisplayName("SCENARIO B & C — Redis cache MISS & PostgreSQL fallback: executes authoritative PostgreSQL reservation")
    void testRedisCacheMissFallsBackToPostgres() {
        String eventId = "EVT-REDIS-MISS-002";
        String paymentId = "PAY-REDIS-002";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-2",
                "amount", 200.0,
                "currency", "USD"
        ));

        // Cache starts empty
        assertThat(idempotencyCacheService.isCompleted(eventId)).isFalse();

        // Process through idempotency engine
        ProcessingResult result = idempotencyEngine.process(envelope);

        assertThat(result).isEqualTo(ProcessingResult.SUCCESS);

        // Verify PostgreSQL authoritative state
        Optional<ProcessedEventEntity> eventOpt = processedEventRepository.findByEventId(eventId);
        assertThat(eventOpt).isPresent();
        assertThat(eventOpt.get().getStatus()).isEqualTo(EventStatus.COMPLETED);

        // Verify business mutation persisted
        Optional<PaymentEntity> paymentOpt = paymentRepository.findByPaymentId(paymentId);
        assertThat(paymentOpt).isPresent();
        assertThat(paymentOpt.get().getAmount()).isEqualByComparingTo(new BigDecimal("200.0"));
    }

    @Test
    @DisplayName("SCENARIO D — Redis OUTAGE: fail-open design allows processing to succeed via PostgreSQL")
    void testRedisOutageFailsOpenAndSucceedsViaPostgres() {
        String eventId = "EVT-REDIS-OUTAGE-003";
        String paymentId = "PAY-REDIS-003";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-3",
                "amount", 300.0,
                "currency", "USD"
        ));

        // Create failing RedisTemplate mock simulating Redis connection failure / timeout
        StringRedisTemplate failingTemplate = mock(StringRedisTemplate.class);
        when(failingTemplate.opsForValue()).thenThrow(new RedisConnectionFailureException("Simulated Redis connection refused"));

        IdempotencyCacheService failingCacheService = new RedisIdempotencyCacheService(failingTemplate, idempotencyProperties);

        // Verify fail-open contract directly on cache service
        assertThat(failingCacheService.isCompleted(eventId)).isFalse();
        failingCacheService.markCompleted(eventId); // must not throw

        // Build IdempotencyEngine with failing cache service to verify end-to-end fail-open processing
        IdempotencyEngine resilientEngine = new IdempotencyEngineImpl(
                idempotencyService,
                transactionalBusinessExecutor,
                retryClassifier,
                failingCacheService
        );

        ProcessingResult result = resilientEngine.process(envelope);

        // Must succeed using PostgreSQL despite Redis outage
        assertThat(result).isEqualTo(ProcessingResult.SUCCESS);

        Optional<ProcessedEventEntity> eventOpt = processedEventRepository.findByEventId(eventId);
        assertThat(eventOpt).isPresent();
        assertThat(eventOpt.get().getStatus()).isEqualTo(EventStatus.COMPLETED);

        Optional<PaymentEntity> paymentOpt = paymentRepository.findByPaymentId(paymentId);
        assertThat(paymentOpt).isPresent();
    }

    @Test
    @DisplayName("SCENARIO E — Cache populated AFTER successful commit: completed transaction creates Redis entry")
    void testCachePopulatedAfterSuccessfulCommit() {
        String eventId = "EVT-REDIS-POPULATE-004";
        String paymentId = "PAY-REDIS-004";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-4",
                "amount", 400.0,
                "currency", "USD"
        ));

        ProcessingResult result = idempotencyEngine.process(envelope);
        assertThat(result).isEqualTo(ProcessingResult.SUCCESS);

        // Verify Redis contains the completed key with exact format
        String key = "event-processing:" + eventId;
        String cachedValue = stringRedisTemplate.opsForValue().get(key);
        assertThat(cachedValue).isEqualTo("COMPLETED");

        // Verify TTL is present and greater than 0
        Long ttl = stringRedisTemplate.getExpire(key, TimeUnit.SECONDS);
        assertThat(ttl).isNotNull();
        assertThat(ttl).isGreaterThan(0);
    }

    @Test
    @DisplayName("SCENARIO F — Failed / rolled-back transaction does NOT create completed cache entry")
    void testFailedTransactionDoesNotCreateCacheEntry() {
        String eventId = "EVT-REDIS-FAILED-005";
        String paymentId = "PAY-REDIS-005";
        // Invalid amount causes domain processor to throw InvalidEventPayloadException
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-5",
                "amount", -50.0,
                "currency", "USD"
        ));

        assertThatThrownBy(() -> idempotencyEngine.process(envelope))
                .isInstanceOf(InvalidEventPayloadException.class);

        // Verify Redis does NOT contain key
        String key = "event-processing:" + eventId;
        Boolean hasKey = stringRedisTemplate.hasKey(key);
        assertThat(hasKey).isFalse();

        // Verify business mutation was not persisted
        Optional<PaymentEntity> paymentOpt = paymentRepository.findByPaymentId(paymentId);
        assertThat(paymentOpt).isEmpty();
    }

    @Test
    @DisplayName("SCENARIO G & H — TTL expiry & duplicate after expiry: falls back to PostgreSQL and prevents duplicate mutation")
    void testTtlExpiryFallsBackToPostgresAndPreventsDuplicate() {
        String eventId = "EVT-REDIS-TTL-006";
        String paymentId = "PAY-REDIS-006";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-6",
                "amount", 600.0,
                "currency", "USD"
        ));

        // 1. Initial processing completes and populates cache
        ProcessingResult firstResult = idempotencyEngine.process(envelope);
        assertThat(firstResult).isEqualTo(ProcessingResult.SUCCESS);
        assertThat(idempotencyCacheService.isCompleted(eventId)).isTrue();

        // 2. Simulate TTL expiry by manually evicting/deleting key
        idempotencyCacheService.evict(eventId);
        assertThat(idempotencyCacheService.isCompleted(eventId)).isFalse();

        // 3. Duplicate arrives after cache expiry: falls back to PostgreSQL
        ProcessingResult secondResult = idempotencyEngine.process(envelope);

        // PostgreSQL authoritatively detects COMPLETED status and skips duplicate
        assertThat(secondResult).isEqualTo(ProcessingResult.SKIPPED_DUPLICATE);

        // Verify exactly 1 payment record exists
        long paymentCount = paymentRepository.findAll().stream()
                .filter(p -> paymentId.equals(p.getPaymentId()))
                .count();
        assertThat(paymentCount).isEqualTo(1);

        // Verify Redis cache was re-populated by PostgreSQL fallback
        assertThat(idempotencyCacheService.isCompleted(eventId)).isTrue();
    }

    @Test
    @DisplayName("SCENARIO I — Concurrent duplicate delivery: multiple concurrent requests commit exactly one business effect")
    void testConcurrentDuplicateDeliveryUnderRedis() throws Exception {
        String eventId = "EVT-REDIS-CONC-007";
        String paymentId = "PAY-REDIS-007";
        EventEnvelope envelope = EventEnvelope.of(eventId, "PAYMENT_COMPLETED", Map.of(
                "paymentId", paymentId,
                "userId", "USER-7",
                "amount", 700.0,
                "currency", "USD"
        ));

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger duplicateCount = new AtomicInteger(0);
        AtomicInteger exceptionCount = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    ProcessingResult result = idempotencyEngine.process(envelope);
                    if (result == ProcessingResult.SUCCESS) {
                        successCount.incrementAndGet();
                    } else if (result == ProcessingResult.SKIPPED_DUPLICATE) {
                        duplicateCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    exceptionCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            }));
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(15, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        executor.shutdown();

        // Exactly one thread must successfully execute the business effect
        assertThat(successCount.get()).isEqualTo(1);
        assertThat(duplicateCount.get() + exceptionCount.get()).isEqualTo(threadCount - 1);

        // Exactly one payment must be persisted in PostgreSQL
        long paymentCount = paymentRepository.findAll().stream()
                .filter(p -> paymentId.equals(p.getPaymentId()))
                .count();
        assertThat(paymentCount).isEqualTo(1);

        // Redis cache must be populated
        assertThat(idempotencyCacheService.isCompleted(eventId)).isTrue();
    }
}
