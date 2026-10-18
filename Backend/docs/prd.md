# Product Requirements Document (PRD)
## Idempotent Event Processing System

---

### 1. Executive Summary & Problem Statement

In distributed, event-driven architectures (specifically systems utilizing Apache Kafka), message delivery semantics default to **at-least-once delivery**. A message published to a broker may be delivered and consumed by worker services multiple times due to:
- Kafka consumer group rebalances,
- Network timeouts, partition failures, or transient socket disconnects,
- Producer retries upon unacknowledged writes,
- Consumer crash/restart prior to committing consumer offsets,
- Downstream service or database timeouts prompting Kafka retry mechanisms.

If an event carries state-mutating business intent (such as `PAYMENT_COMPLETED` creating payments or `ORDER_CREATED` creating orders), processing duplicate messages without strict safeguards leads to duplicate financial charges, duplicate shipments, and corrupted business records.

**The Core Invariant of this Project:**
$$\text{At-Least-Once Delivery} + \text{Idempotent Processing} = \text{Exactly-Once Business Effect}$$

The **Idempotent Event Processing System** is an enterprise-grade backend service engineered to guarantee that regardless of how many times a message is delivered (sequentially, sporadically, or concurrently across consumer threads), the associated business operation is executed **exactly once**.

---

### 2. Project Goals & Target Audience

#### 2.1 Goals
- Guarantee strong idempotency for event processing using PostgreSQL as the durable source of truth.
- Provide a Redis-based fast-path lookup optimization for previously completed events without compromising durability.
- Safely resolve high-concurrency race conditions (e.g., identical event arriving simultaneously across multiple consumer threads or pods) without using naive `SELECT -> check -> INSERT` patterns.
- Implement non-blocking, multi-tier retries with an explicit Dead Letter Queue (DLQ) for poison pills and unrecoverable failures.
- Provide end-to-end auditability, state tracking, and structured observability.
- Deliver a clean, production-oriented reference architecture suitable for enterprise event backends.

#### 2.2 Target Audience
- Senior Backend Engineers, Distributed Systems Architects, and Technical Leads.
- Technical evaluators and interviewers assessing production-grade distributed systems design in Java/Spring Boot.

---

### 3. Functional Requirements

#### FR-1: Event Ingestion (REST API)
- Provide a secure HTTP endpoint `POST /api/events` to accept business events.
- Validate payload structure: mandatory `eventId` (non-blank unique identifier), valid `eventType`, and non-empty `payload`.
- Asynchronously publish validated events to the primary Kafka topic (`events`).
- Record the event with durable status `SUBMITTED`.
- Provide an event status query endpoint `GET /api/events/{eventId}` returning processing status, retry count, timestamps, and business outcome.

#### FR-2: Event Transport & Messaging
- Use Apache Kafka to decouple ingestion from background processing.
- Set `eventId` as the Kafka record key to provide partition affinity and preserve ordering for events sharing the same key.
- **Clarification:** Kafka partitioning does NOT guarantee idempotency. The durable idempotency guarantee comes strictly from PostgreSQL unique constraints, transactional business processing, and the durable `processed_events` state table.
- Provide clean event serialization/deserialization using Jackson JSON.

#### FR-3: Idempotency & Deduplication Engine
- Track all events in a dedicated persistence table (`processed_events`).
- Enforce strict database-level unique constraints on `event_id`.
- Use the following durable `EventStatus` values consistently:
  - `SUBMITTED`: The REST request was validated and the event was successfully published to Kafka.
  - `RECEIVED`: The Kafka consumer received the event from the broker.
  - `PROCESSING`: The event is actively being processed inside the business processing workflow.
  - `COMPLETED`: The business operation and corresponding event state were successfully committed.
  - `FAILED`: The event reached a terminal failure state and is routed to the DLQ.
- Do NOT introduce `RETRYING` as a durable PostgreSQL `EventStatus`. Kafka retry mechanisms manage retry attempts, and retry count is tracked via the `retry_count` column.
- **Redis Role:** Redis provides a fast-path optimization for previously completed events. Redis is NOT the authoritative idempotency mechanism; PostgreSQL remains the durable source of truth. Redis loss, eviction, restart, or outage must never compromise idempotency.
- If Redis is unavailable or returns a cache miss, the system transparently falls back to PostgreSQL.

#### FR-4: Business Processing & Scope
- Provide modular, pluggable business processors for supported event types:
  - `PAYMENT_COMPLETED`:
    1. Validates the payment payload.
    2. Creates the payment record in PostgreSQL (`payments` table).
    3. Updates the corresponding `processed_events` state to `COMPLETED`.
    4. Commits both operations atomically within the same database transaction.
    - **Scope Constraint:** The initial release must NOT implement account balance management. Do NOT introduce an `accounts` table, `wallet` table, `balance` column, balance service, wallet service, ledger service, or additional financial domain models unless explicitly requested later.
  - `ORDER_CREATED`:
    1. Validates the order payload.
    2. Creates the order record in PostgreSQL (`orders` table).
    3. Updates the corresponding `processed_events` state to `COMPLETED`.
    4. Commits both operations atomically.
- Business operations and event status transitions must execute inside atomic database transactions (`@Transactional`).
- If an event has already reached `COMPLETED`, skip the business logic immediately.

#### FR-5: Retry Mechanism & Dead Letter Queue (DLQ)
- Differentiate between transient exceptions (e.g., database connection timeout, transient lock failure) and non-transient exceptions (e.g., malformed payload, invalid business rule).
- Transient errors undergo controlled exponential backoff retry via Kafka retry mechanisms.
- Upon reaching maximum retry attempts (default: 3), route the failed event along with comprehensive failure metadata (stack trace excerpt, failure timestamp, original topic, headers) to `events.dlq` and record durable status as `FAILED`.

---

### 4. Non-Functional Requirements

| Category | Requirement Specification |
| :--- | :--- |
| **Consistency** | Strong transactional consistency (ACID) for business entities and idempotency audit records within PostgreSQL. |
| **Idempotency Invariant** | For any unique `eventId`, the underlying business state mutation must occur strictly once. A naive `SELECT -> check -> INSERT` pattern is strictly forbidden. |
| **Fault Tolerance** | No message loss under broker restarts, consumer crashes, or transient database outages. |
| **Resilience** | System continues functioning safely even if Redis is degraded, flushed, or offline (graceful degradation to PostgreSQL). |
| **Performance Targets** | *Benchmark Goals (not strict functional acceptance criteria):* Sub-5ms duplicate filtering when Redis is hot; sub-25ms end-to-end processing under normal database load. Functional correctness and idempotency take strict precedence over latency benchmarks. |
| **Observability** | Structured logging with Mapped Diagnostic Context (MDC) propagating `eventId`, `eventType`, and trace IDs across all layers. |
| **Auditability** | Full lineage of event processing: creation time, execution duration, retry count, failure stack, and terminal status. |

---

### 5. Event Data Model

#### 5.1 Canonical Event Format
All incoming and published events adhere to the following JSON structure:

```json
{
  "eventId": "EVT-8f92-4d1e-92b1-5a6390141f22",
  "eventType": "PAYMENT_COMPLETED",
  "timestamp": "2026-09-07T12:00:00.000Z",
  "payload": {
    "paymentId": "PAY-9001",
    "userId": "USER-4412",
    "amount": 500.00,
    "currency": "USD"
  }
}
```

#### 5.2 Supported Initial Event Types
1. `PAYMENT_COMPLETED`:
   - Payload: `paymentId` (String), `userId` (String), `amount` (BigDecimal), `currency` (String).
2. `ORDER_CREATED`:
   - Payload: `orderId` (String), `customerId` (String), `totalAmount` (BigDecimal), `itemCount` (Integer).

---

### 6. Idempotency Scenarios to Solve

The system is rigorously designed against the following failure and concurrency patterns:

1. **Scenario A (Normal Processing):** Event arrives once $\rightarrow$ validated $\rightarrow$ processed $\rightarrow$ marked `COMPLETED` $\rightarrow$ downstream state committed.
2. **Scenario B (Sequential Duplicate):** Event arrives $\rightarrow$ marked `COMPLETED` $\rightarrow$ same event arrives seconds or hours later $\rightarrow$ fast-path or DB check intercepts $\rightarrow$ business logic skipped $\rightarrow$ logged as redundant duplicate.
3. **Scenario C (High-Frequency Burst):** An upstream bug fires 100 identical events in rapid succession $\rightarrow$ First event acquires lock / processing state $\rightarrow$ remaining 99 are safely skipped without duplicate execution.
4. **Scenario D (Concurrent Duplicate Race):** Two consumer threads pull identical messages simultaneously $\rightarrow$ A naive `SELECT -> check -> INSERT` is forbidden. PostgreSQL unique constraint on `event_id` along with an isolated transaction boundary serializes execution $\rightarrow$ exactly one executes the business transaction; the second catches the constraint conflict cleanly, observes status, and skips business execution.
5. **Scenario E (Failure & Retry):** Processing fails midway (transient DB error) $\rightarrow$ transaction rolls back completely (no partial business state) $\rightarrow$ event retry initiated $\rightarrow$ subsequent attempt succeeds.
6. **Scenario F (Consumer Crash / Offset Not Committed):** Business processing commits to DB $\rightarrow$ worker crashes before Kafka `commitOffset` $\rightarrow$ new worker re-consumes message $\rightarrow$ DB status check finds `COMPLETED` $\rightarrow$ consumer commits offset without re-executing business logic.
7. **Scenario G (Application Restart):** Whole application terminates and restarts $\rightarrow$ durable state in PostgreSQL guarantees historical duplicates are never re-executed.

---

### 7. Tech Stack & Boundaries

- **Language:** Java 21 (LTS)
- **Framework:** Spring Boot 3.3.x / 3.x
- **Persistence & ORM:** Spring Data JPA, Hibernate, PostgreSQL 16
- **Messaging:** Apache Kafka, Spring for Apache Kafka
- **Cache & Fast Lookup:** Redis (Lettuce client), Spring Data Redis
- **Testing:** JUnit 5, Mockito, AssertJ, Testcontainers (PostgreSQL, Kafka)
- **Build Tool:** Apache Maven

#### Strict Architectural Boundaries (Out of Scope)
- ❌ No Kubernetes / Helm charts (Docker & Docker Compose are used for local and staging containerization).
- ❌ No NoSQL document stores (MongoDB, Couchbase).
- ❌ No Search clusters (Elasticsearch, OpenSearch).
- ❌ No microservice sprawl (single clean modular monolith with distinct domain separation).
- ❌ No frontend UI in initial release.
- ❌ No account balances, wallets, or ledger tables in initial release.

---

### 8. Acceptance & Success Criteria

1. **Deterministic Idempotency:** 100 concurrent submissions of the same `eventId` result in exactly 1 payment record created in PostgreSQL.
2. **Zero In-Memory Fragility:** Completely restarting the Spring Boot JVM retains 100% duplicate protection.
3. **Safe Retries:** Flaky operations retry up to configured limits before landing cleanly in `events.dlq` with status `FAILED`.
4. **Resilience to Cache Loss:** Flushing or stopping Redis results in zero duplicate business effects; PostgreSQL absorbs deduplication.
5. **Automated Verification:** All integration tests pass via Testcontainers without mock database instances.
