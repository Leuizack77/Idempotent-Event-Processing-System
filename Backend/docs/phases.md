# Implementation Phases Breakdown
## Idempotent Event Processing System

---

### Phase Management Governance
- **Strictly Sequential Phases:** Only the actively approved phase may be developed.
- **No Speculative Implementation:** Do not implement future-phase features early, do not create speculative classes, and do not introduce unnecessary abstractions.
- **Phase Sign-Off Gate:** No phase is marked complete until its acceptance criteria and automated tests are fully passing.
- **Context Preservation:** Upon completion of each implementation phase, `docs/memory.md` must be updated before requesting user approval for the next phase.

---

### Phase 1: Project Setup and Spring Boot Foundation
- **Objective:** Establish the Maven build system, Java 21 toolchain, Spring Boot 3.x baseline, project structure, and basic health endpoints.
- **Tasks:**
  - Create `pom.xml` with dependencies: Web, Data JPA, Validation, Actuator, Flyway, PostgreSQL, Kafka, Redis, Testcontainers.
  - Create base package `com.example.eventprocessor` and `EventProcessorApplication.java`.
  - Configure `application.yml` with profiles (`default`, `docker`, `test`).
  - Configure standard logging formats.
- **Files Likely to Change:**
  - `pom.xml`
  - `src/main/resources/application.yml`
  - `src/main/java/com/example/eventprocessor/EventProcessorApplication.java`
  - `src/test/java/com/example/eventprocessor/EventProcessorApplicationTests.java`
- **Dependencies:** None.
- **Acceptance Criteria:**
  - Application compiles cleanly with Java 21 (`mvn clean compile`).
  - Spring Context loads successfully in unit tests (`mvn test`).
  - Actuator `/actuator/health` endpoint responds.
- **Tests Required:** `EventProcessorApplicationTests.java` (contextLoads).
- **Completion Criteria:** Zero build errors, clean test run.

---

### Phase 2: PostgreSQL and Event Persistence
- **Objective:** Design and implement the relational schema, Flyway migrations, JPA entities, and repositories for event state tracking and business records.
- **Tasks:**
  - Write Flyway migration `V1__create_processed_events_table.sql` with columns: `id`, `event_id` (UNIQUE), `event_type`, `status`, `retry_count`, `error_message`, `processed_at`, `created_at`, `updated_at`.
  - Write Flyway migration `V2__create_payments_table.sql` for payment business entity: `id`, `payment_id` (UNIQUE), `user_id`, `amount`, `currency`, `status`, `created_at`. (Do NOT create `accounts` or `wallet` tables).
  - Create JPA entities: `ProcessedEventEntity` and `PaymentEntity`.
  - Create Spring Data JPA repositories: `ProcessedEventRepository` and `PaymentRepository`.
  - Define enum `EventStatus` with exact values: `SUBMITTED`, `RECEIVED`, `PROCESSING`, `COMPLETED`, `FAILED`. (No `RETRYING` enum; retry count is tracked via `retry_count` column).
- **Files Likely to Change:**
  - `src/main/resources/db/migration/V1__create_processed_events_table.sql`
  - `src/main/resources/db/migration/V2__create_payments_table.sql`
  - `src/main/java/com/example/eventprocessor/entity/ProcessedEventEntity.java`
  - `src/main/java/com/example/eventprocessor/entity/PaymentEntity.java`
  - `src/main/java/com/example/eventprocessor/idempotency/EventStatus.java`
  - `src/main/java/com/example/eventprocessor/repository/ProcessedEventRepository.java`
  - `src/main/java/com/example/eventprocessor/repository/PaymentRepository.java`
- **Dependencies:** Phase 1.
- **Acceptance Criteria:**
  - Migrations execute cleanly against PostgreSQL.
  - Unique index constraint on `event_id` prevents duplicate inserts at the database engine level.
- **Tests Required:** Repository integration test using Testcontainers PostgreSQL verifying unique constraint enforcement.
- **Completion Criteria:** Repository tests pass against real PostgreSQL container.

---

### Phase 3: REST Event Ingestion API
- **Objective:** Create the public REST API for submitting events and checking their idempotency/processing status.
- **Tasks:**
  - Define request DTO `EventRequestDto` with `@NotBlank`, `@Valid`, and generic payload mapping.
  - Define response DTOs `EventResponseDto`, `EventStatusResponseDto`.
  - Implement `GlobalExceptionHandler` handling `MethodArgumentNotValidException` and domain errors (RFC 7807 format).
  - Implement `EventIngestionController` with:
    - `POST /api/events` (Validates, persists initial `SUBMITTED` record, and returns `202 Accepted`).
    - `GET /api/events/{eventId}` (Returns processing status, retry count, and details).
  - Implement `EventIngestionService` stub (ready for Kafka integration).
- **Files Likely to Change:**
  - `src/main/java/com/example/eventprocessor/dto/*`
  - `src/main/java/com/example/eventprocessor/controller/EventIngestionController.java`
  - `src/main/java/com/example/eventprocessor/service/EventIngestionService.java`
  - `src/main/java/com/example/eventprocessor/exception/GlobalExceptionHandler.java`
- **Dependencies:** Phase 1, Phase 2.
- **Acceptance Criteria:**
  - Invalid payloads return `400 Bad Request` with descriptive field validation messages.
  - Valid payloads return `202 Accepted` with status `SUBMITTED`.
  - `GET /api/events/{eventId}` returns `404 Not Found` for unknown events or `200 OK` for recorded events.
- **Tests Required:** WebMvc unit tests (`MockMvc`) for controller validations, status queries, and error mappings.
- **Completion Criteria:** All controller tests pass.

---

### Phase 4: Kafka Producer and Consumer
- **Objective:** Configure Kafka topics, serialization, producer delivery, and consumer ingestion.
- **Tasks:**
  - Configure `KafkaConfig` with producer/consumer properties, Jackson serialization/deserialization, consumer group ID, and manual offset ack configuration.
  - Define topic constants: `events`, `events.retry`, `events.dlq`.
  - Implement `KafkaEventProducer` to publish messages with `eventId` as the record key (for partition affinity).
  - Implement `KafkaEventConsumer` to listen to `events` topic and manage manual offset acknowledgment.
  - Connect `EventIngestionService` to produce Kafka events upon REST acceptance.
- **Files Likely to Change:**
  - `src/main/java/com/example/eventprocessor/config/KafkaConfig.java`
  - `src/main/java/com/example/eventprocessor/producer/KafkaEventProducer.java`
  - `src/main/java/com/example/eventprocessor/consumer/KafkaEventConsumer.java`
  - `src/main/java/com/example/eventprocessor/service/EventIngestionServiceImpl.java`
- **Dependencies:** Phase 2, Phase 3.
- **Acceptance Criteria:**
  - Events accepted via REST are published to Kafka topic `events` with `key = eventId`.
  - Consumer successfully polls and deserializes event envelopes without offset loss.
- **Tests Required:** Kafka integration test using Testcontainers Kafka verifying end-to-end publish-and-consume.
- **Completion Criteria:** Message delivery verified with real Kafka container.

---

### Phase 5: Core Idempotency Implementation
- **Objective:** Implement the durable idempotency engine preventing duplicate execution using PostgreSQL and safe transaction boundaries.
- **Tasks:**
  - Implement `IdempotencyService` without naive `SELECT -> check -> INSERT` patterns:
    - Safe state transition to `PROCESSING` within an isolated transaction boundary (`REQUIRES_NEW`).
    - Catches `DataIntegrityViolationException` cleanly without corrupting the caller transaction with rollback-only state.
    - Transitions to `COMPLETED` or `FAILED`.
  - Integrate idempotency check into consumer workflow.
- **Files Likely to Change:**
  - `src/main/java/com/example/eventprocessor/idempotency/IdempotencyService.java`
  - `src/main/java/com/example/eventprocessor/consumer/KafkaEventConsumer.java`
- **Dependencies:** Phase 2, Phase 4.
- **Acceptance Criteria:**
  - Sequential duplicates for the same `eventId` are detected and skipped.
  - Consumer safely acknowledges Kafka offset without re-executing business operations.
- **Tests Required:** Integration tests verifying sequential duplicate bypass and clean exception handling.
- **Completion Criteria:** Tests confirm duplicate arrivals produce zero duplicate operations.

---

### Phase 6: Transactional Business Processing
- **Objective:** Implement domain business operations within atomic database transactions alongside idempotency updates.
- **Tasks:**
  - Implement `BusinessEventProcessor` strategy interface.
  - Implement `PaymentEventProcessor` for `PAYMENT_COMPLETED` events:
    - Validates payment payload.
    - Creates the payment record in `payments`.
    - Updates `processed_events` status to `COMPLETED`.
    - Commits both operations atomically within the same database transaction.
    - **Scope Constraint:** Does NOT update balances, wallets, or ledger tables.
  - Implement `OrderEventProcessor` for `ORDER_CREATED` events (extensibility proof).
  - Implement `EventProcessorRegistry` to dispatch events to appropriate processor based on `eventType`.
- **Files Likely to Change:**
  - `src/main/java/com/example/eventprocessor/processing/*`
  - `src/main/java/com/example/eventprocessor/service/PaymentService.java`
  - `src/main/resources/db/migration/V3__create_orders_table.sql`
- **Dependencies:** Phase 5.
- **Acceptance Criteria:**
  - Both business table mutation and `processed_events` status commit atomically.
  - If business operation throws an error, the database rolls back completely (no partial records).
- **Tests Required:** Transactional rollback integration tests; business execution tests.
- **Completion Criteria:** Verified atomic commit and rollback behavior.

---

### Phase 7: Retry and Dead Letter Topic (DLQ)
- **Objective:** Build robust retry handling for transient errors and Dead Letter Queue routing for poison pills.
- **Tasks:**
  - Classify exceptions into retryable (transient) and non-retryable (validation/fatal).
  - Configure Kafka `DefaultErrorHandler` with exponential backoff (e.g., 3 retries).
  - Route exhausted messages to `events.dlq` using `DeadLetterPublishingRecoverer`.
  - Populate DLQ message headers with diagnostic metadata (`X-Exception-Message`, `X-Retry-Count`).
  - Implement `KafkaDlqConsumer` to monitor and record DLQ messages in `processed_events` with status `FAILED`.
- **Files Likely to Change:**
  - `src/main/java/com/example/eventprocessor/config/KafkaRetryConfig.java`
  - `src/main/java/com/example/eventprocessor/consumer/KafkaDlqConsumer.java`
- **Dependencies:** Phase 6.
- **Acceptance Criteria:**
  - Transient failures retry up to max attempts then publish to `events.dlq`.
  - Fatal errors bypass retries and route directly to DLQ.
  - `processed_events` status is updated to `FAILED`.
- **Tests Required:** Integration tests injecting transient and fatal failures, verifying DLQ contents and header metadata.
- **Completion Criteria:** DLQ and retry mechanisms pass automated tests.

---

### Phase 8: Redis Optimization
- **Objective:** Add Redis fast-path caching to deduplicate previously completed events with low latency.
- **Tasks:**
  - Configure `RedisConfig` with Lettuce connection factory and `RedisTemplate<String, String>`.
  - Implement `IdempotencyCacheService` to query and set key `event:idempotency:{eventId}` with TTL (24 hours).
  - Update `KafkaEventConsumer` to check Redis before hitting PostgreSQL.
  - Add fallback logic: if Redis is offline, flushed, or errors, log warning and fall back to PostgreSQL seamlessly without compromising idempotency.
- **Files Likely to Change:**
  - `src/main/java/com/example/eventprocessor/config/RedisConfig.java`
  - `src/main/java/com/example/eventprocessor/idempotency/IdempotencyCacheService.java`
  - `src/main/java/com/example/eventprocessor/consumer/KafkaEventConsumer.java`
- **Dependencies:** Phase 7.
- **Acceptance Criteria:**
  - Repeated event arrival hits Redis fast-path and skips DB queries.
  - If Redis is stopped or fails, the system still guarantees idempotency using PostgreSQL without error.
- **Tests Required:** Fast-path cache hit test; Redis outage fallback test.
- **Completion Criteria:** Redis acceleration and fallback resilience confirmed via tests.

---

### Phase 9: Concurrency and Failure Testing
- **Objective:** Subject the entire system to high-concurrency race condition testing and failure scenarios.
- **Tasks:**
  - Write multi-threaded concurrency test (`ConcurrentDuplicateProcessingTest`) using `ExecutorService` and `CountDownLatch` simulating 50 concurrent threads processing the same `eventId`.
  - Write consumer crash simulation test (uncommitted offset re-delivery).
  - Write application restart simulation test verifying durable state in PostgreSQL.
- **Files Likely to Change:**
  - `src/test/java/com/example/eventprocessor/concurrency/*`
- **Dependencies:** Phase 8.
- **Acceptance Criteria:**
  - In concurrent tests, exactly 1 payment is created; all 49 other threads detect duplicate and skip.
  - Restart tests verify idempotency holds across JVM lifecycles.
- **Tests Required:** Multi-threaded stress and race-condition integration tests.
- **Completion Criteria:** Zero duplicate side effects under concurrent pressure.

---

### Phase 10: Docker and Docker Compose
- **Objective:** Containerize the application and orchestrate all supporting infrastructure (PostgreSQL, Kafka, Redis).
- **Tasks:**
  - Write multi-stage `Dockerfile` optimizing build cache and image size with Eclipse Temurin 21 JRE.
  - Write `docker-compose.yml` defining services: `app`, `postgres`, `kafka` (KRaft mode), and `redis`.
  - Configure environment variables and healthchecks for dependent startup order.
  - Document local run instructions.
- **Files Likely to Change:**
  - `Dockerfile`
  - `docker-compose.yml`
  - `.env.example`
- **Dependencies:** Phase 9.
- **Acceptance Criteria:**
  - `docker compose up -d` brings up all services cleanly.
  - API accepts requests and processes events successfully within the container environment.
- **Tests Required:** Smoke test of containerized stack using `curl`.
- **Completion Criteria:** Fully functional standalone Docker Compose environment.

---

### Phase 11: Observability and Production-Readiness
- **Objective:** Add enterprise logging, correlation IDs, Actuator metrics, and operational health probes.
- **Tasks:**
  - Configure MDC logging filter propagating `correlationId`, `eventId`, and `eventType`.
  - Configure Micrometer metrics tracking: `events.processed.total`, `events.duplicate.total`, `events.dlq.total`, `events.processing.duration`.
  - Expose Actuator endpoints (`/actuator/health`, `/actuator/metrics`, `/actuator/prometheus`).
- **Files Likely to Change:**
  - `src/main/java/com/example/eventprocessor/config/MdcFilter.java`
  - `src/main/java/com/example/eventprocessor/config/MetricsConfig.java`
  - `src/main/resources/application.yml`
- **Dependencies:** Phase 10.
- **Acceptance Criteria:**
  - Logs show correlated `eventId` across HTTP and Kafka consumer threads.
  - Metrics accurately increment on duplicate detection and DLQ transitions.
- **Tests Required:** Verification of Actuator endpoints and log structure.
- **Completion Criteria:** Observability verified and documented.

---

### Phase 12: Documentation and Interview Readiness
- **Objective:** Finalize documentation, architecture diagrams, interview talking points, and verification guide.
- **Tasks:**
  - Create complete `README.md` with system overview, architecture diagram, setup guide, curl examples, and test instructions.
  - Write `docs/interview_guide.md` answering key technical questions: At-least-once vs Exactly-once, handling race conditions, distributed transactions vs local idempotency, Redis vs DB tradeoffs.
- **Files Likely to Change:**
  - `README.md`
  - `docs/interview_guide.md`
- **Dependencies:** Phase 11.
- **Acceptance Criteria:**
  - Documentation completely reflects implemented codebase.
- **Completion Criteria:** Project ready for production review and interview presentation.
