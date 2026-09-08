# Engineering Rules & AI Agent Contract
## Idempotent Event Processing System

---

### 1. Fundamental Principles & Governing Hierarchy

This document forms the binding **engineering contract** for any AI agent or software engineer contributing to this repository. All code, refactoring, and test additions must adhere strictly to these rules.

If requirements appear to conflict, resolve them using this exact precedence hierarchy:
1. **docs/prd.md** (Product Requirements)
2. **docs/architecture.md** (System Architecture & Decisions)
3. **docs/rules.md** (Engineering Rules & Constraints)
4. **docs/phases.md** (Active Phase Scope)

> [!CAUTION]
> **Conflict Protocol:**
> If a contradiction or ambiguity is discovered across documents or requirements, the AI agent must **STOP and report it immediately** instead of silently choosing an interpretation.

---

### 2. Technology & Dependency Boundaries

- **Target Platform:** Java 21 (LTS) and Spring Boot 3.3.x / 3.x.
- **Allowed Core Dependencies:**
  - `spring-boot-starter-web`
  - `spring-boot-starter-data-jpa`
  - `spring-boot-starter-data-redis`
  - `spring-kafka`
  - `spring-boot-starter-validation`
  - `org.postgresql:postgresql`
  - `org.flywaydb:flyway-core`
  - `org.flywaydb:flyway-database-postgresql`
  - `org.projectlombok:lombok` (optional)
  - `spring-boot-starter-actuator`
  - `testcontainers` (PostgreSQL, Kafka)
- **Forbidden Dependencies & Frameworks:**
  - ❌ Do NOT add Kubernetes / Helm configurations.
  - ❌ Do NOT add Elasticsearch, Solr, or MongoDB.
  - ❌ Do NOT introduce secondary backend frameworks (e.g., Micronaut, Quarkus).
  - ❌ Do NOT split the application into separate microservice repos or multi-module maven projects unless instructed.

---

### 3. Business Scope Boundaries (PAYMENT_COMPLETED)

The initial release must **NOT** implement account balance management.

The initial `PAYMENT_COMPLETED` processor should strictly and only:
1. Validate the payment payload.
2. Create the payment record in PostgreSQL (`payments` table).
3. Update the corresponding `processed_events` state to `COMPLETED`.
4. Commit both operations atomically.

> [!IMPORTANT]
> **Forbidden Financial Domain Additions:**
> Do NOT introduce an `accounts` table, `wallet` table, `balance` column, balance service, wallet service, ledger service, or additional financial domain models unless explicitly requested in a future phase.

---

### 4. Idempotency, Concurrency & Persistence Rules

#### 4.1 Durable Source of Truth
- **PostgreSQL is the Sole Authoritative Source:** In-memory maps/sets (e.g., `ConcurrentHashMap`, `HashSet`) are strictly forbidden as idempotency mechanisms.
- **Redis Role:** Redis provides a fast-path optimization for previously completed events. Redis is NOT the authoritative idempotency mechanism. Redis loss, eviction, restart, or outage must NEVER compromise idempotency.
- **Durable Uniqueness:** Every idempotency key (`event_id`) must have a database-level unique constraint (`CONSTRAINT uq_processed_events_event_id UNIQUE (event_id)`).

#### 4.2 Concurrency Safeguards & Transaction Boundaries
- **Naive Check-Then-Act Forbidden:** A naive `SELECT -> check -> INSERT` approach is strictly forbidden.
- **Transaction Safety:** 
  - Do NOT catch a `DataIntegrityViolationException` inside an active `@Transactional` method and then blindly continue using the same rollback-only transaction.
  - State reservation / unique checks must occur within an isolated transaction boundary (e.g., `Propagation.REQUIRES_NEW`) so that constraint collisions roll back cleanly without poisoning subsequent reads or outer flows.
- **Non-Negotiable Guarantees:**
  - Exactly one successful business effect.
  - Duplicate attempts do not execute the business effect.
  - PostgreSQL remains the final durable protection.
  - Transaction rollback cannot leave inconsistent state or partial records.

#### 4.3 Kafka Transport Guarantees
- `eventId` is used as the Kafka record key to provide partition affinity and preserve ordering for events sharing the same key.
- Kafka partitioning is **NOT** the mechanism that guarantees idempotency. Durable idempotency is guaranteed strictly by PostgreSQL unique constraints, transactional business processing, and durable `processed_events` state.

#### 4.4 Event Status Consistency
The system must use only these durable `EventStatus` values in PostgreSQL:
- `SUBMITTED`
- `RECEIVED`
- `PROCESSING`
- `COMPLETED`
- `FAILED`

`RETRYING` must **NOT** be used as a durable `EventStatus`. Kafka retry mechanisms manage retry attempts, with the count tracked in `retry_count`.

#### 4.5 Performance & Latency Guidelines
Any latency references (such as sub-5ms or sub-25ms) are benchmark target goals, NOT strict functional acceptance criteria. Functional correctness and durable idempotency strictly take precedence over latency.

---

### 5. Code Quality & API Conventions

- **Mandatory Constructor Injection:** Never use field injection (`@Autowired` on fields). Always use explicit constructor injection (or `@RequiredArgsConstructor`).
- **Encapsulation & Immutability:** Keep fields `private final`. Use Java `record` types for DTOs and event envelopes.
- **DTO Isolation:** Never expose JPA Entities directly via REST endpoints.
- **Input Validation:** Enforce Bean Validation (`jakarta.validation.constraints.*`) on all request models.
- **No Swallowed Exceptions:** Never write empty catch blocks or swallow exceptions. Handle errors centrally via `@RestControllerAdvice`.
- **Sanitized Client Errors:** Never expose raw stack traces or internal DB schema to clients.
- **Contextual Logging (MDC):** Every log statement in the event pipeline must include `traceId`, `eventId`, and `eventType`.
- **Behavioral Testing:** Use Testcontainers for real PostgreSQL and Kafka integration tests. A multi-threaded concurrency test verifying 50 concurrent threads produce exactly 1 business effect is mandatory.

---

### 6. Phase Execution & AI Agent Discipline

- **Strictly Sequential Phases:** Only the currently approved phase may be implemented.
- **No Speculative Coding:** Do NOT implement future-phase functionality early. Do NOT create speculative classes or unnecessary abstractions for future phases.
- **No Unnecessary Modifications:** Do NOT modify working code from completed phases unless integration requires it.
- **Verification Gate:** Before declaring a phase complete:
  1. Compile code (`mvn clean compile`).
  2. Execute all relevant tests (`mvn test`).
  3. Confirm all phase acceptance criteria are met.
  4. Update `docs/memory.md` with state, decisions, and tests run.
  5. Wait for explicit user instruction before advancing to the next phase.

---

### 7. Memory Tracking Rules (`docs/memory.md`)

- Do **NOT** create or maintain `docs/memory.md` during documentation-only tasks.
- `docs/memory.md` becomes mandatory once actual implementation begins (Phase 1).
- After every completed implementation phase, `docs/memory.md` must be updated to track:
  - What was completed
  - Files changed
  - Important technical decisions
  - Tests executed and test results
  - Known limitations
  - Current phase
  - Next phase awaiting user approval
