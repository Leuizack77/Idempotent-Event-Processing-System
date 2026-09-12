# Distributed Systems Interview Guide: Idempotent Event Processing System

> **Staff & Senior Engineer Interview Preparation Guide**
> Practical, in-depth architectural questions and answers derived directly from the implementation of this production-grade idempotent event processing engine.

---

## Structure of Each Topic
Every topic in this guide is formatted to simulate an actual technical interview conversation:
1. **The Interview Question**
2. **Short Interview Answer (30-Second Elevator Pitch):** Direct, punchy, and confident summary.
3. **Deep Technical Explanation:** The architectural and distributed systems theory.
4. **How This Project Implements It:** Specific classes, methods, database queries, and configuration used.
5. **Likely Follow-Up Question:** How an interviewer will drill deeper.
6. **Strong Follow-Up Answer:** Nuanced, senior-level response addressing edge cases and tradeoffs.

---

## Table of Contents
- [1. At-Least-Once Delivery vs. Exactly-Once Processing](#1-at-least-once-delivery-vs-exactly-once-processing)
- [2. Why Kafka Record Keying Is Not Idempotency](#2-why-kafka-record-keying-is-not-idempotency)
- [3. Two-Tier Idempotency Architecture](#3-two-tier-idempotency-architecture)
- [4. Why Redis Cannot Be the Authoritative Source of Truth](#4-why-redis-cannot-be-the-authoritative-source-of-truth)
- [5. Concurrency & Race Conditions (Concurrent Stampedes)](#5-concurrency--race-conditions-concurrent-stampedes)
- [6. Why Naive `SELECT -> check -> INSERT` Is Fundamentally Broken](#6-why-naive-select---check---insert-is-fundamentally-broken)
- [7. Lease-Based Processing & Crash Recovery](#7-lease-based-processing--crash-recovery)
- [8. Zombie Workers & Split-Brain Execution](#8-zombie-workers--split-brain-execution)
- [9. Fencing Tokens & Optimistic Rollback](#9-fencing-tokens--optimistic-rollback)
- [10. Transaction Boundaries & Why `REQUIRES_NEW` Matters](#10-transaction-boundaries--why-requires_new-matters)
- [11. Distributed Transactions (2PC) vs. Localized Idempotency](#11-distributed-transactions-2pc-vs-localized-idempotency)
- [12. Exception Classification: Retryable vs. Non-Retryable Poison Pills](#12-exception-classification-retryable-vs-non-retryable-poison-pills)
- [13. Exponential Backoff & Jitter Under Transient Failures](#13-exponential-backoff--jitter-under-transient-failures)
- [14. Dead Letter Queue (DLQ) Topology & Diagnostic Metadata](#14-dead-letter-queue-dlq-topology--diagnostic-metadata)
- [15. Kafka Offset Acknowledgment Strategy (`MANUAL_IMMEDIATE`)](#15-kafka-offset-acknowledgment-strategy-manual_immediate)
- [16. Consumer Crash & Partition Rebalance Dynamics](#16-consumer-crash--partition-rebalance-dynamics)
- [17. Distributed Tracing, MDC Context & OpenMetrics Observability](#17-distributed-tracing-mdc-context--openmetrics-observability)
- [18. Containerized Deployment Architecture & Health Probes](#18-containerized-deployment-architecture--health-probes)
- [19. Architectural Tradeoffs & Production Limitations](#19-architectural-tradeoffs--production-limitations)
- [20. End-to-End System Walkthrough](#20-end-to-end-system-walkthrough)
- [Executive Pitch: 60-Second & 5-Minute Project Explanations](#executive-pitch-60-second--5-minute-project-explanations)

---

### 1. At-Least-Once Delivery vs. Exactly-Once Processing

#### Question
> "How does your system achieve 'exactly-once' processing when consuming from Kafka?"

#### Short Interview Answer
> "We don't claim end-to-end exactly-once messaging transport. Kafka provides **at-least-once** delivery. We achieve **exactly-once business effect** by combining database unique constraints, atomic transactional reservation, UUID fencing tokens, and post-commit deduplication caching at the persistence layer."

#### Deep Explanation
Kafka's transactional producer (`enable.idempotence=true` and `KafkaTransactionManager`) only guarantees exactly-once semantics *between Kafka topics* (read-process-write loops within Kafka). The moment an event triggers external side effects—such as writing to a relational database or calling a payment gateway—Kafka's internal transaction cannot coordinate across the network without a costly, fragile two-phase commit (2PC). Therefore, distributed architects decouple transport from persistence: allow the messaging broker to deliver messages *at least once*, and guarantee idempotency within the transactional boundary of the downstream storage engine.

#### How This Project Implements It
1. `KafkaConfig` sets producer `enable.idempotence=true` and `acks=all` (preventing broker-side duplicates on publish retries).
2. The consumer explicitly accepts duplicate Kafka deliveries.
3. `IdempotencyEngineImpl` intercepts every record before invoking business logic.
4. If an event has already committed (`EventStatus.COMPLETED`), the engine marks it as a skipped duplicate and acknowledges the Kafka offset without invoking `PaymentEventProcessor`.

#### Likely Follow-Up Question
> "If Kafka already has an idempotent producer, why is that insufficient for downstream consumers?"

#### Strong Follow-Up Answer
> "Kafka's idempotent producer assigns a Producer ID (PID) and sequence numbers to records. The broker rejects duplicate publishes from that specific producer session. However, once records sit in the topic, consumer-side issues—such as a worker crashing before committing its consumer offset, a partition rebalance, or an HTTP client resending an API request with the same business ID—cause consumer-side duplicate deliveries that Kafka's producer idempotency knows nothing about."

---

### 2. Why Kafka Record Keying Is Not Idempotency

#### Question
> "In Kafka, you partition by `eventId`. Doesn't partition affinity guarantee that duplicate events are processed in order and avoid race conditions?"

#### Short Interview Answer
> "No. Keying preserves ordering within a single partition, but it does not protect against consumer crashes, uncommitted offset redeliveries, consumer group rebalancing, or multi-threaded parallel consumers within the same node."

#### Deep Explanation
When `eventId` is used as the Kafka message key, Kafka's default murmur2 partitioner maps that key to a deterministic partition. In steady state, a single consumer thread in a consumer group consumes that partition.
However:
1. If the consumer thread crashes after persisting the payment but *before* committing the Kafka offset, Kafka redelivers the record.
2. During a consumer group rebalance (e.g., node autoscaling or deployment rollout), partitions move between instances. The new owner re-consumes from the last committed offset.
3. If the REST ingestion client retries after a network timeout, the duplicate message is appended later in the log.
Thus, partition affinity is a helpful ordering mechanism, never an idempotency guarantee.

#### How This Project Implements It
* `KafkaEventProducer` sets `key = eventId` to optimize partition affinity.
* Tests in `KafkaProducerConsumerIntegrationTest` and `KafkaFailureAndCrashIntegrationTest` explicitly verify uncommitted offset redeliveries and partition rebalances, proving that deduplication correctness relies entirely on PostgreSQL and Redis, not Kafka keying.

#### Likely Follow-Up Question
> "What happens if two consumers read from different partitions with the same event ID?"

#### Strong Follow-Up Answer
> "While proper keying routes the same ID to the same partition, if topic partitions are repartitioned or a client mistakenly sends messages with different keys, both events race simultaneously. Our PostgreSQL unique constraint `uq_processed_events_event_id` and atomic conditional SQL updates ensure that even across concurrent partitions or concurrent instances, exactly one transaction wins and the other is safely rejected."

---

### 3. Two-Tier Idempotency Architecture

#### Question
> "Describe your deduplication pipeline. Why did you choose a two-tier approach with Redis and PostgreSQL?"

#### Short Interview Answer
> "We combine an in-memory Redis cache (Tier 1) for sub-millisecond duplicate suppression during high-throughput duplicate storms with an ACID PostgreSQL relational store (Tier 2) as the immutable, authoritative source of truth."

#### Deep Explanation
A single storage engine represents a compromise:
* **Relational DBs (PostgreSQL):** Provide ACID transactions, unique constraints, and durable row locking, but disk I/O and connection pools become bottlenecks if subjected to massive duplicate traffic (e.g., 5,000 duplicate requests/sec).
* **Distributed In-Memory Stores (Redis):** Provide sub-millisecond latencies, but lack durable relational transactions and risk split-brain, eviction, or data loss on cluster reboot.
By combining both, Redis acts as a fast-path filter shielding PostgreSQL from 99% of read queries on completed events, while PostgreSQL provides 100% mathematical correctness.

#### How This Project Implements It
1. **Tier 1 (Redis Fast-Path):** `IdempotencyCacheService.isCompleted(eventId)` queries Redis for `event-processing:{eventId}`. If `"COMPLETED"`, the request terminates immediately with `ProcessingResult.SKIPPED_DUPLICATE`.
2. **Tier 2 (PostgreSQL Reservation):** On a Redis cache miss, execution falls back to `IdempotencyServiceImpl.tryReserve()`, which uses PostgreSQL row-level locks and unique constraints.
3. **Post-Commit Sync:** `IdempotencyEngineImpl` writes to Redis **only after** the PostgreSQL transaction successfully commits.

#### Likely Follow-Up Question
> "Why don't you put the pending `PROCESSING` state in Redis as a distributed lock?"

#### Strong Follow-Up Answer
> "Distributed locks in Redis (like Redlock) suffer from GC pause edge cases, network partitioning, and clock drift vulnerabilities. More importantly, acquiring a lock in Redis and writing to PostgreSQL creates a dual-write problem. By storing the `PROCESSING` state and business data in the same PostgreSQL database, the reservation and state transitions share the exact same ACID transaction engine as our business entities."

---

### 4. Why Redis Cannot Be the Authoritative Source of Truth

#### Question
> "What happens if Redis crashes, flushes, or evicts an idempotency key?"

#### Short Interview Answer
> "Nothing bad happens. Redis is strictly non-authoritative and fail-open. If Redis fails, all requests fall back directly to PostgreSQL. Idempotency guarantees remain 100% intact."

#### Deep Explanation
Redis operates under memory boundaries. Keys are subject to LRU/LFU eviction policies, TTL expirations, and instance restarts. Treating Redis as authoritative means that an evicted key allows a duplicate message to re-execute business logic (e.g., double-charging a credit card). Furthermore, a network partition between the application and Redis should not prevent the system from processing events if the primary database is healthy.

#### How This Project Implements It
1. `RedisIdempotencyCacheService` wraps all Lettuce operations in try-catch blocks. If Redis throws `RedisConnectionException` or times out, it logs a warning and returns `false` (cache miss).
2. The consumer proceeds to PostgreSQL without throwing an error or routing to DLQ.
3. If an evicted key arrives, PostgreSQL detects the existing `COMPLETED` row in `processed_events`, repopulates Redis with a 24-hour TTL, and skips execution.
4. Tested in `ResilienceAndFencingIntegrationTest.testScenario8_RealRedisOutage_FailsOpenCleanlyToPostgres_UnderActiveConcurrency()` using the native Docker API to pause the Redis container during live 25-worker concurrency.

#### Likely Follow-Up Question
> "Could a failed post-commit write to Redis cause inconsistent state?"

#### Strong Follow-Up Answer
> "No. If the PostgreSQL transaction commits and the subsequent Redis `SET` fails due to network loss, the worst-case scenario is a subsequent duplicate will miss Redis and query PostgreSQL. PostgreSQL will see `status = COMPLETED`, acknowledge the duplicate, and retry writing to Redis. Correctness is never compromised."

---

### 5. Concurrency & Race Conditions (Concurrent Stampedes)

#### Question
> "Two worker threads receive duplicate copies of the same event at the exact same millisecond. Walk me through how you prevent both from executing the business logic."

#### Short Interview Answer
> "Both workers miss the Redis cache and attempt an atomic conditional `UPDATE` on PostgreSQL. PostgreSQL's row-level lock forces one transaction to wait. The winner updates the status to `PROCESSING` and acquires a UUID fencing token; the loser's query matches zero rows, inspects the locked state, sees active processing, and is rejected with `EventProcessingInProgressException`."

#### Deep Explanation
Concurrency must be solved at the database engine level, not in application memory. In `READ COMMITTED` isolation, when two transactions attempt to update the same row simultaneously, the second transaction blocks until the first transaction commits or rolls back. Once the winner commits `status = 'PROCESSING'`, the loser re-evaluates the `WHERE` clause:
```sql
WHERE event_id = :eventId AND (status IN ('SUBMITTED', 'RECEIVED', 'FAILED') OR (status = 'PROCESSING' AND updated_at < :staleThreshold))
```
Because the status is now `PROCESSING` and `updated_at` is fresh, the loser's update affects `0` rows.

#### How This Project Implements It
1. `ProcessedEventRepository.transitionToProcessing()` executes the atomic update.
2. If `0` rows are updated, `IdempotencyServiceImpl` re-queries the row.
3. Detecting that another worker holds an active lease, it returns `ReservationResult.activeProcessing(eventId)`.
4. `IdempotencyEngineImpl` throws `EventProcessingInProgressException`.
5. The Kafka consumer catches the exception and suppresses `acknowledgment.acknowledge()`, allowing Kafka to back off and redeliver later.
6. Stress-tested in `HighConcurrencyIdempotencyIntegrationTest` with 50 threads racing on a single uncached event (resulting in exactly 1 payment created).

#### Likely Follow-Up Question
> "What if the row doesn't exist yet in the database when the two workers race?"

#### Strong Follow-Up Answer
> "We avoid check-then-act entirely. We execute a native SQL `INSERT INTO processed_events (...) VALUES (...) ON CONFLICT (event_id) DO NOTHING`. The database engine guarantees only one insert succeeds. The loser gets an update count of 0, realizes the conflict occurred, reads the winner's committed row, and handles it cleanly without throwing an unhandled exception."

---

### 6. Why Naive `SELECT -> check -> INSERT` Is Fundamentally Broken

#### Question
> "Why can't we simply write: `if (!repo.existsByEventId(id)) { repo.save(event); process(); }`?"

#### Short Interview Answer
> "Because that introduces a classical Time-of-Check to Time-of-Use (TOCTOU) race window. Under concurrency, two threads will both execute the `SELECT`, both see that the record does not exist, both proceed to execute business mutations, and then collide on insert, creating duplicate business effects."

#### Deep Explanation
Between the execution of the `SELECT` query and the subsequent `INSERT`, the calling thread yields or executes I/O. Any other thread executing concurrently reads the exact same empty result.
Even if a unique constraint exists on `event_id`, if the business logic (e.g., calling Stripe or creating an order) is invoked *before* or *during* the insert, the duplicate business effect has already occurred. Furthermore, catching a `DataIntegrityViolationException` inside a Spring `@Transactional` method poisons the Hibernate Session, setting `RollbackOnly` and causing subsequent operations in that context to fail with `UnexpectedRollbackException`.

#### How This Project Implements It
* Eliminates check-then-act in favor of atomic SQL statements: `INSERT ... ON CONFLICT DO NOTHING` and `UPDATE ... WHERE ...`.
* Wraps the reservation step in `Propagation.REQUIRES_NEW`, isolating constraint evaluation completely from the business processing transaction.

#### Likely Follow-Up Question
> "Could we solve this with Java synchronization or a `ReentrantLock`?"

#### Strong Follow-Up Answer
> "Java in-memory locks only synchronize threads within a single JVM process. In a production environment with multiple container instances or autoscaling Kubernetes pods, JVM locks provide zero cross-instance protection. Distributed synchronization must be anchored in shared persistent infrastructure."

---

### 7. Lease-Based Recovery

#### Question
> "What happens if a worker reserves an event, transitions it to `PROCESSING`, and then immediately crashes (e.g., OOM killer or host hardware failure)?"

#### Short Interview Answer
> "The event is protected by a time-bounded processing lease. When another consumer picks up the redelivered message after the lease duration (default: 60 seconds) has elapsed, it atomically reclaims the stale lease, increments the retry count, issues a new fencing token, and resumes processing."

#### Deep Explanation
Without a lease, a crashed worker leaves the event permanently trapped in `PROCESSING` status. Subsequent duplicate arrivals would indefinitely believe another worker is actively working on it.
To fix this safely:
1. Every reservation stamps `updated_at = NOW()`.
2. A lease duration $T_{\text{lease}}$ (e.g., 60s) defines stale threshold: $\text{staleThreshold} = \text{NOW}() - T_{\text{lease}}$.
3. The reservation query explicitly includes stale events:
   `OR (status = 'PROCESSING' AND updated_at < :staleThreshold)`.

#### How This Project Implements It
* Configured via `idempotency.processing-lease-duration` (in `IdempotencyProperties`).
* `ProcessedEventRepository.transitionToProcessing()` reclaims stale rows and updates `retry_count = retry_count + 1`.
* Tested in `IdempotencyEngineIntegrationTest.testStaleProcessingLeaseReclaimed()` where an event in `PROCESSING` with `updated_at` older than 60s is successfully reclaimed by a new worker.

#### Likely Follow-Up Question
> "What if the initial worker was not dead, but merely paused during a 65-second Garbage Collection pause?"

#### Strong Follow-Up Answer
> "That is the classic 'zombie worker' problem. If Worker A pauses for 65 seconds, Worker B reclaims the expired lease and begins processing. When Worker A wakes up, it assumes it still owns the task. This is precisely why lease recovery must always be paired with **fencing tokens**."

---

### 8. Zombie Workers & Split-Brain Execution

#### Question
> "Explain the 'zombie worker' problem and how your architecture guarantees data safety when it occurs."

#### Short Interview Answer
> "A zombie worker is a process presumed dead whose lease expired and was reassigned, but which unexpectedly awakens and attempts to complete its write. We neutralize zombie workers using monotonic UUID fencing tokens verified inside the final atomic business transaction."

#### Deep Explanation
In distributed systems, you cannot reliably distinguish between a crashed process and a slow process (asynchronous network model). If Worker A takes 70 seconds on a 60-second lease, Worker B legitimately reclaims the lease. If Worker A subsequently attempts to write to the database, you have a split-brain write collision unless Worker A is fenced out.

#### How This Project Implements It
1. When Worker B reclaims the lease, it overwrites the `processing_token` in `processed_events` with `TOKEN-B`.
2. When Worker A awakens and invokes `TransactionalBusinessExecutor`, its SQL update asserts its own token:
   ```sql
   UPDATE processed_events SET status = 'COMPLETED'
   WHERE event_id = :eventId AND processing_token = 'TOKEN-A';
   ```
3. Because the token in the database is now `TOKEN-B`, the update matches `0` rows.
4. The executor detects `updated == 0` and throws `StaleProcessingLeaseException`.
5. Spring rolls back Worker A's database transaction completely, discarding any pending mutations to `payments` or `orders`.
6. Tested in `ResilienceAndFencingIntegrationTest.testScenario7_LeaseExpirationAndFencingToken_PreventsStaleWorkerCompletion()`.

#### Likely Follow-Up Question
> "Why use a UUID for fencing instead of a monotonically increasing integer sequence?"

#### Strong Follow-Up Answer
> "A monotonic counter requires reading and incrementing an integer in lockstep across distributed nodes. A UUIDv4 provides cryptographically secure 128-bit collision resistance generated in memory without coordinating with a central sequence generator, while still providing strict equality fencing (`token = :candidateToken`)."

---

### 9. Fencing Tokens & Optimistic Rollback

#### Question
> "How does the fencing token integrate with Spring's `@Transactional` boundary?"

#### Short Interview Answer
> "The business mutations and the fencing token assertion execute within the exact same database transaction (`@Transactional(REQUIRED)`). If the fencing update affects 0 rows, we throw an exception, forcing a complete transaction rollback and guaranteeing that no partial business records can ever be committed by a stale worker."

#### Deep Explanation
If business table insertion (`INSERT INTO payments`) and idempotency completion (`UPDATE processed_events`) occurred in separate transactions, a stale worker could commit the payment before realizing its token was invalid. By binding both operations to the same relational transaction, ACID atomicity ensures that either *both* the payment and the `COMPLETED` status commit, or *neither* commits.

#### How This Project Implements It
In `TransactionalBusinessExecutor.java`:
```java
@Transactional(propagation = Propagation.REQUIRED)
public void executeAndComplete(EventEnvelope envelope, String processingToken) {
    businessProcessor.process(envelope); // inserts payment/order
    int updated = processedEventRepository.updateStatus(
        eventId, EventStatus.COMPLETED, EventStatus.PROCESSING, processingToken, now, null, now
    );
    if (updated == 0) {
        throw new StaleProcessingLeaseException(eventId, processingToken);
    }
}
```

#### Likely Follow-Up Question
> "What happens to the Kafka offset when a `StaleProcessingLeaseException` is thrown?"

#### Strong Follow-Up Answer
> "The exception bubbles out of `KafkaEventConsumer`, bypassing offset acknowledgment. The consumer listener container invokes the error handler. The error handler's `markFailedOnRecovery` explicitly inspects for `StaleProcessingLeaseException` and suppresses writing `FAILED` to the database, ensuring that Worker B's active ownership is never corrupted by Worker A's cleanup."

---

### 10. Transaction Boundaries & Why `REQUIRES_NEW` Matters

#### Question
> "Why does `IdempotencyService.tryReserve()` use `Propagation.REQUIRES_NEW` instead of joining the existing caller transaction?"

#### Short Interview Answer
> "To prevent constraint collisions and reservation rollbacks from poisoning the caller's persistence context with a `RollbackOnly` status."

#### Deep Explanation
In Spring Data JPA / Hibernate, if a database query throws an exception or causes a constraint violation inside an active `@Transactional` boundary, Hibernate marks the entire transaction as `RollbackOnly`. Even if the developer wraps the code in a `try-catch` block, any subsequent attempt to commit the outer transaction will throw an `UnexpectedRollbackException`.
By isolating reservation in `REQUIRES_NEW`:
1. Reservation opens its own physical database transaction and commits or rolls back immediately.
2. The outer consumer workflow retains a clean, unpoisoned session.
3. The outer transaction can proceed with clean business operations or handle duplicate branches gracefully.

#### How This Project Implements It
* Annotated on `IdempotencyServiceImpl.tryReserve()`:
  `@Transactional(propagation = Propagation.REQUIRES_NEW)`.
* Verified under concurrency tests to guarantee zero unexpected rollback exceptions.

#### Likely Follow-Up Question
> "What is the overhead of using `REQUIRES_NEW` for reservation?"

#### Strong Follow-Up Answer
> "It requires acquiring a separate database connection from the HikariCP connection pool for the duration of the reservation method. Because reservation is an ultra-fast indexed primary key query that completes in single-digit milliseconds, the connection is released almost instantaneously back to the pool."

---

### 11. Distributed Transactions (2PC) vs. Localized Idempotency

#### Question
> "Why didn't you use XA / Two-Phase Commit (2PC) to coordinate Kafka offset commits and PostgreSQL writes?"

#### Short Interview Answer
> "2PC is notoriously slow, introduces high latency, creates single points of failure with blocking coordinator locks, and is not natively supported by Kafka. Localized idempotency provides equal data correctness with vastly superior throughput, availability, and partition tolerance."

#### Deep Explanation
Under the CAP and PACELC theorems, 2PC sacrifices availability and latency to achieve consistency across heterogeneous systems. If a network partition occurs while a coordinator holds locks across Kafka and PostgreSQL, entire partitions stall.
Instead, industry-standard event-driven architectures embrace **BASE** (Basically Available, Soft state, Eventual consistency):
1. Treat the messaging broker as an at-least-once transport.
2. Commit the business side effects and idempotency state atomically in the local database.
3. Acknowledge the broker offset asynchronously afterward.
4. If the process crashes before step 3, redeliver and let local idempotency safely deduplicate.

#### How This Project Implements It
* We explicitly avoid JTA/XA.
* Local transaction atomicity is achieved in PostgreSQL.
* Kafka offset is acknowledged via `acknowledgment.acknowledge()` strictly post-commit.

#### Likely Follow-Up Question
> "How does this compare to the Transactional Outbox Pattern?"

#### Strong Follow-Up Answer
> "The Transactional Outbox pattern is designed for the *producer* side (ensuring a database write and Kafka publish happen atomically). Our architecture addresses the *consumer* side (ensuring incoming Kafka events produce idempotent database writes). Both patterns share the same philosophy: avoid distributed transactions by anchoring atomicity in the local relational database."

---

### 12. Exception Classification: Retryable vs. Non-Retryable Poison Pills

#### Question
> "How does your consumer handle poison pills versus transient infrastructure outages?"

#### Short Interview Answer
> "We classify exceptions using `KafkaRetryClassifier`. Fatal errors like schema violations bypass retries and route immediately to the DLQ. Transient errors like connection timeouts undergo exponential backoff retries."

#### Deep Explanation
A **poison pill** is a message that will fail 100% of the time regardless of how many times it is retried (e.g., malformed JSON, negative payment amount, missing mandatory user ID). Retrying poison pills wastes CPU cycles, pollutes logs, delays legitimate messages on that partition, and risks crashing consumers.
Conversely, **transient errors** (e.g., Hikari connection pool exhaustion, temporary network blip to PostgreSQL, deadlock victim) will succeed once the transient condition clears.

#### How This Project Implements It
1. `KafkaRetryClassifier` defines:
   * **Non-Retryable:** `InvalidEventException`, `InvalidEventPayloadException`, `DuplicateBusinessEntityException`.
   * **Retryable:** Connection timeouts, SQL deadlocks, I/O timeouts.
2. `KafkaConfig` registers non-retryable exceptions on Spring Kafka's `DefaultErrorHandler`:
   `errorHandler.addNotRetryableExceptions(...)`.
3. Non-retryable exceptions skip backoff and trigger `DeadLetterPublishingRecoverer` instantly.
4. Tested in `KafkaRetryDlqIntegrationTest`.

#### Likely Follow-Up Question
> "What happens to the event status in PostgreSQL when a poison pill fails immediately?"

#### Strong Follow-Up Answer
> "The error handler's composite recoverer invokes `markFailedOnRecovery`, which updates `processed_events` to status `FAILED` and records the exact root-cause message in `error_message`, giving support teams complete visibility into why the payload was rejected."

---

### 13. Exponential Backoff & Jitter Under Transient Failures

#### Question
> "Explain your retry backoff algorithm and its configuration parameters."

#### Short Interview Answer
> "We use an exponential backoff policy configured with an initial interval of 1 second, a multiplier of 2.0, a maximum interval of 10 seconds, and a maximum of 3 attempts."

#### Deep Explanation
Immediate retries during downstream database distress (e.g., connection spikes) cause a "thundering herd" problem that exacerbates the outage. Exponential backoff progressively widens the delay between retry attempts ($1\text{s} \rightarrow 2\text{s} \rightarrow 4\text{s}$), giving overloaded databases or network buffers time to recover.

#### How This Project Implements It
Configured in `application.yml` and `KafkaRetryProperties.java`:
```yaml
kafka:
  retry:
    max-attempts: 3
    initial-interval: 1000ms
    multiplier: 2.0
    max-interval: 10000ms
```
Injected into Spring's `DefaultErrorHandler` via `ExponentialBackOff`.

#### Likely Follow-Up Question
> "Why is max attempts capped at 3 instead of 10 or 20?"

#### Strong Follow-Up Answer
> "Because Kafka consumer threads hold partition assignments. If a consumer blocks for too long retrying an event (exceeding `max.poll.interval.ms`), the Kafka coordinator presumes the consumer is dead and triggers an aggressive partition rebalance. 3 attempts provide sufficient window for transient recovery while keeping partition latency low and preventing rebalance storms."

---

### 14. Dead Letter Queue (DLQ) Topology & Diagnostic Metadata

#### Question
> "What information do you attach to messages routed to your Dead Letter Queue, and why?"

#### Short Interview Answer
> "We enrich DLQ records with comprehensive diagnostic headers including the root exception message, exception class, original topic, partition, offset, timestamp, retry count, trace ID, and correlation ID."

#### Deep Explanation
A DLQ message stripped of context is an operational nightmare; engineers cannot diagnose why a message failed without correlating disparate logs. Enriched headers make DLQ messages self-describing, enabling automated dead-letter processing, operational alerting, and replay tooling.

#### How This Project Implements It
`DeadLetterPublishingRecoverer.addHeadersFunction()` attaches:
* `X-Exception-Message`: Root cause error string.
* `X-Exception-Class`: Exception class name.
* `X-Original-Topic`: `events`.
* `X-Original-Partition` & `X-Original-Offset`: Exact coordinate in the primary topic.
* `X-Retry-Count`: Number of attempts made.
* `X-Trace-ID` & `X-Correlation-ID`: Trace context for OpenTelemetry/ELK lookup.
* Topic topology: Dedicated `events.dlq` topic (3 partitions).
* Consumer: `KafkaDlqConsumer` listens to `events.dlq` to log and track dead letters.

#### Likely Follow-Up Question
> "How would you handle redriving or replaying messages from the DLQ back to the primary topic in production?"

#### Strong Follow-Up Answer
> "A production redrive service reads from `events.dlq`, allows an operator to fix the underlying data defect or wait for bug fixes to deploy, strips the DLQ diagnostic headers, and re-publishes the payload back to `events`. Because our downstream pipeline is 100% idempotent, redriving events is completely safe."

---

### 15. Kafka Offset Acknowledgment Strategy (`MANUAL_IMMEDIATE`)

#### Question
> "Why did you choose `AckMode.MANUAL_IMMEDIATE` over Spring's default `BATCH` or auto-commit?"

#### Short Interview Answer
> "Auto-commit commits offsets periodically regardless of whether processing succeeded, risking data loss during crashes. `MANUAL_IMMEDIATE` gives us deterministic, explicit control: we commit the Kafka offset only after the database transaction commits."

#### Deep Explanation
* **Auto-Commit (`enable.auto.commit=true`):** Offsets are committed on a background timer. If a container crashes after polling a batch but before finishing processing, uncommitted messages are lost.
* **Batch Acknowledgment:** Acknowledges the whole batch at the end. A failure in message 5 of 10 causes messages 1 through 4 to be redelivered unnecessarily.
* **`MANUAL_IMMEDIATE`:** The consumer explicitly calls `acknowledgment.acknowledge()` for each record individually as soon as that specific record has been durably committed to the database.

#### How This Project Implements It
* In `KafkaConfig`:
  `factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);`
* In `KafkaEventConsumer`:
  ```java
  ProcessingResult result = idempotencyEngine.process(envelope);
  if (result == ProcessingResult.SUCCESS || result == ProcessingResult.SKIPPED_DUPLICATE) {
      acknowledgment.acknowledge();
  }
  ```

#### Likely Follow-Up Question
> "Doesn't acknowledging every message individually degrade Kafka consumer throughput?"

#### Strong Follow-Up Answer
> "In Spring Kafka, `MANUAL_IMMEDIATE` commits the offset synchronously to the local consumer coordinator map, which commits to the broker in the next poll loop unless configured otherwise. For critical financial events like payments, deterministic consistency strictly takes precedence over raw micro-benchmarked throughput."

---

### 16. Consumer Crash & Partition Rebalance Dynamics

#### Question
> "What happens if a consumer node crashes while processing an event, triggering a group rebalance?"

#### Short Interview Answer
> "Because the offset was not yet acknowledged, Kafka reassigns the partition to a surviving consumer. The new consumer polls the record, discovers either an expired lease to reclaim or an unreserved event, and processes it to completion without duplicate side effects."

#### Deep Explanation
Kafka's group coordinator detects consumer failure via missed heartbeats (`heartbeat.interval.ms` and `session.timeout.ms`). A rebalance revokes partitions and assigns them to active members.
The surviving member reads from the last committed offset. Because the failed node never called `acknowledge()`, the failed event is re-read.
1. If the failed node crashed before writing to PostgreSQL, the new node sees the event as fresh and executes it.
2. If the failed node crashed after committing to PostgreSQL, the new node checks Redis or PostgreSQL, detects `COMPLETED`, acknowledges the offset, and skips business execution.
3. If the failed node crashed mid-transaction, PostgreSQL aborted the uncommitted transaction, leaving the record clean for the new worker.

#### How This Project Implements It
* Verified in `KafkaFailureAndCrashIntegrationTest.testScenario4_ConsumerCrashBeforeOffsetAck_RedeliversAndCompletesIdempotently()` and `testScenario5_MultiPartitionConcurrencyAndDynamicRebalancing()`.

#### Likely Follow-Up Question
> "How does your consumer avoid partition starvation during a rebalance?"

#### Strong Follow-Up Answer
> "We configure cooperative sticky assignors where possible and ensure our processing logic has bounded timeouts. By avoiding blocking external HTTP calls inside the message processing thread, our poll loops remain rapid, keeping heartbeat health consistent."

---

### 17. Distributed Tracing, MDC Context & OpenMetrics Observability

#### Question
> "How do you track a request from the initial REST call through Kafka consumer threads and downstream database writes?"

#### Short Interview Answer
> "We capture or generate a correlation trace ID in a high-precedence servlet filter (`MdcFilter`), propagate it in Kafka record headers, restore it in consumer threads via SLF4J MDC, and expose domain metrics via Micrometer and Prometheus."

#### Deep Explanation
Asynchronous messaging architectures sever HTTP request thread context. Without explicit context propagation:
* Logs on the consumer have no relation to logs on the ingestion API.
* Debugging production issues across microservices requires manual timestamp correlation.
By standardizing on `X-Correlation-ID` and `X-Trace-ID`, the trace identifier travels across HTTP headers, Kafka message metadata, and worker thread pools.

#### How This Project Implements It
1. `MdcFilter` extracts `X-Correlation-ID` or generates `tr-<uuid8>` and binds it to `MDC.put("traceId", traceId)`.
2. `KafkaEventProducer` injects trace headers into Kafka `ProducerRecord`.
3. `KafkaEventConsumer` and `KafkaDlqConsumer` extract headers from `ConsumerRecord` and populate MDC before invoking business logic.
4. Cleaned up in `finally { MDC.clear(); }`.
5. Micrometer metrics instrumented:
   * `events.processed.total`
   * `events.duplicate.total`
   * `events.dlq.total`
   * `events.processing.duration`
6. Actuator endpoint `/actuator/prometheus` exports metrics in OpenMetrics format.

#### Likely Follow-Up Question
> "Why do you call `MDC.clear()` in a `finally` block?"

#### Strong Follow-Up Answer
> "Because Spring web servers (Tomcat) and Kafka listener containers use thread pools. If MDC is not cleared in a `finally` block, thread-local data leaks to the next unrelated request or message processed by that thread, resulting in corrupted trace logs."

---

### 18. Containerized Deployment Architecture & Health Probes

#### Question
> "How is this system containerized, and how do operational probes ensure clean startup?"

#### Short Interview Answer
> "We use a multi-stage Dockerfile with an unprivileged non-root user and Eclipse Temurin 21 JRE. Docker Compose orchestrates PostgreSQL, Kafka (KRaft), Redis, and the app with explicit healthcheck dependencies."

#### Deep Explanation
Container best practices require:
* Minimal attack surface (JRE runtime, not full JDK).
* Running as non-root (`USER spring:spring`).
* Layer caching (copying `pom.xml` and resolving offline before copying source code).
* Coordinated startup: preventing the application container from booting and failing until PostgreSQL, Kafka, and Redis pass their individual health probes.

#### How This Project Implements It
* **`Dockerfile`:**
  * Builder stage: `maven:3.9.8-eclipse-temurin-21-alpine`.
  * Runtime stage: `eclipse-temurin:21-jre-alpine`.
  * Healthcheck probe: `wget -q --spider http://localhost:8080/actuator/health`.
* **`docker-compose.yml`:**
  * `postgres`: `pg_isready` check.
  * `kafka`: `/opt/kafka/bin/kafka-cluster.sh` cluster healthcheck.
  * `redis`: `redis-cli ping` check.
  * `app`: `depends_on` with `condition: service_healthy` across all three dependencies.

#### Likely Follow-Up Question
> "What is the difference between `/actuator/health/liveness` and `/actuator/health/readiness` in this service?"

#### Strong Follow-Up Answer
> "The liveness probe indicates whether the JVM process is alive and responsive; if it fails, the orchestrator should restart the container. The readiness probe verifies whether dependent infrastructure (PostgreSQL connection pool, Kafka broker, Redis) is healthy; if it fails, the orchestrator stops routing incoming HTTP traffic to the node until dependencies recover."

---

### 19. Architectural Tradeoffs & Production Limitations

#### Question
> "Be critical of your own architecture. What are its current limitations, and what would you improve for an ultra-scale production deployment?"

#### Short Interview Answer
> "Our current implementation is single-node focused. For ultra-scale production, we would need multi-AZ PostgreSQL replication with connection pooling proxies, a multi-broker Kafka cluster, distributed Redis Sentinel/Cluster, and horizontal consumer auto-scaling."

#### Deep Explanation
Every architecture represents intentional tradeoffs:
1. **Single-Node PostgreSQL:** PostgreSQL is our authoritative single point of truth. If the primary database crashes without a hot standby, ingestion halts. In production, we would deploy Amazon RDS Multi-AZ or Aurora PostgreSQL with read-replicas.
2. **Connection Pooling Limits:** Using `Propagation.REQUIRES_NEW` creates a second database connection during reservation. Under heavy load, HikariCP pool sizing must be carefully tuned (`maximum-pool-size`) to prevent connection pool exhaustion.
3. **Single REST Endpoint:** We implemented `POST /api/events` but omitted `GET /api/events/{eventId}`. Clients must rely on webhooks, polling external query services, or async notification topics to discover completion.
4. **Local Docker Compose:** Docker Compose is optimized for local developer experience and testing; production requires Kubernetes or AWS ECS with declarative HPA (Horizontal Pod Autoscaling) driven by Kafka consumer lag metrics.

#### Likely Follow-Up Question
> "If database connections become a bottleneck under massive scale, how would you evolve the reservation design?"

#### Strong Follow-Up Answer
> "We could adopt an outbox table design with CDC (Change Data Capture via Debezium), or partition PostgreSQL across database shards keyed by `user_id` or `tenant_id`, ensuring reservation row-locks scale horizontally across multiple database nodes."

---

### 20. End-to-End System Walkthrough

#### Question
> "Walk me step-by-step through the lifecycle of a `PAYMENT_COMPLETED` event from HTTP submission to final completion and DLQ failure."

#### Comprehensive Interview Walkthrough
```text
1. INGESTION:
   - Client sends POST /api/events with JSON payload: {eventId, eventType: "PAYMENT_COMPLETED", payload}.
   - MdcFilter assigns traceId = "tr-a1b2c3d4", binds to MDC, and sets HTTP response header.
   - EventIngestionController validates fields via Bean Validation (@Valid).
   - EventIngestionService inserts initial record into PostgreSQL `processed_events` with status 'SUBMITTED'.
   - KafkaEventProducer publishes EventEnvelope to topic 'events' with key = eventId and trace headers.
   - Controller returns HTTP 202 Accepted.

2. CONSUMPTION & DEDUPLICATION:
   - KafkaEventConsumer polls record from 'events' partition.
   - Consumer extracts trace headers and re-establishes MDC context.
   - Consumer passes envelope to IdempotencyEngine.process().
   - Engine queries Redis: GET event-processing:{eventId}.
     * IF Redis HIT (value == "COMPLETED"):
       - Increments metric events.duplicate.total(source="REDIS").
       - Returns SKIPPED_DUPLICATE.
       - Consumer acknowledges Kafka offset (MANUAL_IMMEDIATE). STOP.

3. POSTGRESQL RESERVATION (ON REDIS MISS):
   - IdempotencyService.tryReserve() opens isolated transaction (REQUIRES_NEW).
   - Attempts atomic SQL transition/insert with candidateToken = UUID.randomUUID().
     * IF already 'COMPLETED' in DB:
       - Returns alreadyCompleted.
       - Engine writes "COMPLETED" to Redis (TTL 24h).
       - Consumer acknowledges Kafka offset. STOP.
     * IF active lease in progress (status == 'PROCESSING' && updated_at < 60s):
       - Throws EventProcessingInProgressException.
       - Kafka offset is NOT acknowledged (broker redelivers). STOP.
     * IF reservation succeeds:
       - Status becomes 'PROCESSING', processing_token = UUID, committed to DB.

4. TRANSACTIONAL BUSINESS EXECUTION:
   - TransactionalBusinessExecutor opens transaction (@Transactional REQUIRED).
   - Dispatches to PaymentEventProcessor.
   - Inserts row into `payments` table (payment_id, user_id, amount, currency).
   - Executes UPDATE processed_events SET status = 'COMPLETED' WHERE event_id = :id AND processing_token = :token.
     * IF token was overwritten by another worker (zombie check):
       - Updated rows == 0.
       - Throws StaleProcessingLeaseException.
       - Spring rolls back transaction (payment discarded). STOP.
     * IF update succeeds:
       - Transaction commits atomically.

5. POST-COMMIT COMPLETION:
   - Engine writes to Redis: SET event-processing:{eventId} "COMPLETED" EX 86400.
   - Increments metric events.processed.total.
   - Consumer invokes acknowledgment.acknowledge() to commit Kafka offset.

6. FAILURE & DLQ PATH (IF BUSINESS LOGIC THROWS):
   - Transient Exception (e.g. timeout):
     * DefaultErrorHandler retries up to 3 times with exponential backoff (1s, 2s, 4s).
   - Fatal Exception (e.g. InvalidEventException) or Retries Exhausted:
     * DeadLetterPublishingRecoverer publishes envelope to topic 'events.dlq'.
     * Attaches diagnostic headers: X-Exception-Message, X-Retry-Count, X-Original-Topic.
     * Error handler updates PostgreSQL `processed_events` status to 'FAILED'.
     * Increments metric events.dlq.total.
     * Acknowledges offset to unblock partition.
```

---

## Executive Pitch: 60-Second & 5-Minute Project Explanations

### The 60-Second Elevator Pitch
> *"I designed and implemented an enterprise-grade Idempotent Event Processing System in Java 21 and Spring Boot that guarantees exactly-once business effects over at-least-once Kafka message delivery. In distributed systems, network retries and consumer crashes cause duplicate messages. Our system prevents double-processing through a two-tier architecture: an in-memory Redis cache provides sub-millisecond duplicate filtering during duplicate storms, while PostgreSQL acts as the immutable, authoritative source of truth using unique constraints, lease-based concurrency control, and UUID fencing tokens. If a worker crashes or pauses during a GC freeze, our fencing tokens prevent zombie workers from committing stale data. If Redis crashes, our fail-open design seamlessly falls back to PostgreSQL without dropping events. The system is containerized with Docker Compose, fully observable via OpenMetrics and MDC distributed tracing, and validated by 67 integration tests covering high-concurrency race conditions and crash simulations."*

### The 5-Minute Architectural Deep-Dive Pitch
> *"When building event-driven microservices around technologies like Apache Kafka, the hardest problem is dealing with the reality of at-least-once delivery. Network glitches during consumer offset commits, broker failovers, and client retries inevitably cause identical messages to be delivered multiple times. If your downstream logic processes payments or creates orders, naive processing will double-charge customers or corrupt inventory.*
>
> *I built this project to demonstrate a mathematically bulletproof, high-throughput solution to that problem. The architecture is built on three core pillars: localized transaction atomicity, two-tier deduplication, and zombie worker protection.*
>
> *First, we intentionally avoid distributed transactions like 2PC, which are notoriously slow and fragile. Instead, we accept at-least-once transport from Kafka and localize atomicity within our relational database, PostgreSQL. When an event arrives, our Idempotency Engine checks a Redis fast-path cache. If the event is already completed, we bypass the database and business logic entirely, acknowledging the Kafka offset in sub-millisecond time.*
>
> *On a cache miss, execution shifts to PostgreSQL. Rather than using an unsafe check-then-act pattern, we execute atomic conditional SQL updates in an isolated `REQUIRES_NEW` transaction. This acquires a time-bounded processing lease and generates a unique UUID fencing token. If multiple worker threads or containers race on the same event at the exact same millisecond, PostgreSQL's row-level locks force one winner; the losers detect active processing and back off.*
>
> *Second, we solved the distributed 'zombie worker' problem. If a worker holding a lease experiences a prolonged 65-second Garbage Collection pause, its 60-second lease expires. Another worker safely reclaims the event with a new token. When the original zombie worker awakens, it executes its business logic inside a Spring `@Transactional` boundary and attempts to mark the event completed using its token. Because the token in the database was replaced, the update affects zero rows. The engine throws a `StaleProcessingLeaseException`, which triggers a complete database rollback of the business mutation. The zombie is fenced out, and data integrity remains 100% intact.*
>
> *Third, we engineered strict resilience and observability. Redis is completely non-authoritative: if Redis goes down, our service fails open to PostgreSQL without errors. Non-retryable poison pills are separated from transient failures, with fatal messages routed to an `events.dlq` topic enriched with root-cause diagnostic headers. Every HTTP request generates an MDC trace ID that is forwarded across Kafka byte headers and restored in consumer threads, and Prometheus metrics monitor throughput, duplicate rates, and processing duration.*
>
> *The entire system runs on Java 21, Spring Boot 3.3.4, PostgreSQL 16, Kafka 3.8, and Redis 7.2. It is fully containerized with healthcheck-orchestrated Docker Compose and backed by 67 integration tests using Testcontainers that subject the engine to 50-thread duplicate stampedes, partition rebalances, and simulated container outages."*
