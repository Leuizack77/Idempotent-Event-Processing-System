# Technical Design & Conventions
## Idempotent Event Processing System

---

### 1. Backend & API Design Principles

The API follows RESTful architectural constraints with enterprise-grade conventions:
- **Stateless Communication:** No server-side HTTP session state.
- **Resource Orientation:** Clear URI paths representing entities and resources (`/api/events`).
- **Standardized Status Codes:** Consistent semantics across ingestion, retrieval, and error scenarios.
- **Strict Input Validation:** Fail-fast validation with detailed feedback on malformed inputs.
- **Immutability:** API contracts are immutable data transfers utilizing Java 21 `record` types.

---

### 2. URI & Endpoint Conventions

All public endpoints are prefixed with `/api`.

| Method | Endpoint | Description | Success Code | Error Codes |
| :--- | :--- | :--- | :--- | :--- |
| `POST` | `/api/events` | Ingest an event into the system | `202 Accepted` | `400 Bad Request`, `503 Service Unavailable` |
| `GET` | `/api/events/{eventId}` | Query processing status & audit | `200 OK` | `404 Not Found`, `500 Internal Error` |
| `GET` | `/actuator/health` | Service liveness & readiness | `200 OK` | `503 Service Unavailable` |
| `GET` | `/actuator/metrics` | System and domain metrics | `200 OK` | `401/403` (if secured) |

---

### 3. JSON Payloads & Schema Conventions

#### 3.1 Ingestion Request Payload (`POST /api/events`)
```json
{
  "eventId": "EVT-1001-A",
  "eventType": "PAYMENT_COMPLETED",
  "payload": {
    "paymentId": "PAY-501",
    "userId": "USER-9921",
    "amount": 250.00,
    "currency": "USD"
  }
}
```

**Field Rules:**
- `eventId`: Mandatory, non-blank string (max 128 chars). Used as the global idempotency key and Kafka message record key.
- `eventType`: Mandatory, non-blank string matching a registered processor type (`PAYMENT_COMPLETED`, `ORDER_CREATED`).
- `payload`: Mandatory, non-empty JSON object containing event-specific attributes.
  - For `PAYMENT_COMPLETED`: `paymentId`, `userId`, `amount`, `currency`. (Note: The system does NOT manage account balances or wallet tables).
  - For `ORDER_CREATED`: `orderId`, `customerId`, `totalAmount`, `itemCount`.

#### 3.2 Ingestion Response Payload (`202 Accepted`)
```json
{
  "eventId": "EVT-1001-A",
  "status": "SUBMITTED",
  "message": "Event accepted and queued for asynchronous processing",
  "submittedAt": "2026-09-07T12:00:00.123Z"
}
```

#### 3.3 Event Status Query Response (`GET /api/events/{eventId}`)
```json
{
  "eventId": "EVT-1001-A",
  "eventType": "PAYMENT_COMPLETED",
  "status": "COMPLETED",
  "retryCount": 0,
  "errorMessage": null,
  "processedAt": "2026-09-07T12:00:01.450Z",
  "createdAt": "2026-09-07T12:00:00.120Z",
  "updatedAt": "2026-09-07T12:00:01.450Z"
}
```

---

### 4. Event Status Lifecycle & State Machine

The system uses the following durable `EventStatus` values in PostgreSQL:

- `SUBMITTED`: The REST request was validated and the event was successfully published to Kafka.
- `RECEIVED`: The Kafka consumer received the event from the broker.
- `PROCESSING`: The event is actively being processed inside the business processing workflow.
- `COMPLETED`: The business operation and corresponding event state were successfully committed.
- `FAILED`: The event reached a terminal failure state and is routed to the DLQ.

```mermaid
stateDiagram-v2
    [*] --> SUBMITTED: POST /api/events validated & published
    SUBMITTED --> RECEIVED: Kafka Consumer dequeues event
    RECEIVED --> PROCESSING: Acquired isolated state lock in DB
    PROCESSING --> COMPLETED: Business operation & event state committed
    PROCESSING --> FAILED: Retries exhausted or fatal non-transient error (Routed to DLQ)
    COMPLETED --> [*]
    FAILED --> [*]
```

> [!IMPORTANT]
> **No RETRYING Status in PostgreSQL:**
> `RETRYING` is deliberately NOT an `EventStatus` value. Retry attempts are orchestrated dynamically by Kafka consumer retry mechanisms, and the attempt count is recorded in the `retry_count` integer column in `processed_events`.

---

### 5. Concurrency, Locking & Fencing Design

#### 5.1 Prohibition of Naive Check-Then-Act
A naive `SELECT -> check -> INSERT` approach is strictly forbidden because concurrent consumer threads will both observe an absent record and concurrently proceed with duplicate business execution.

#### 5.2 Safe Transaction Boundaries & Ownership Fencing
- Concurrent duplicate handling relies strictly on PostgreSQL's unique constraint (`uq_processed_events_event_id`), isolated transaction boundaries (`REQUIRES_NEW`), and explicit **fencing tokens** (`processing_token`).
- **Rule on Exceptions:** Do not implement a `DataIntegrityViolationException` catch inside an active Spring `@Transactional` method and then blindly continue executing queries in that same rollback-only transaction.

#### 5.3 Fencing Token Lifecycle & Ownership Validation
To prevent a slow or stalled worker from overwriting state after its lease expires and another worker has reclaimed the event:
1. **Processing Ownership:** Every time an event is reserved or reclaimed into `PROCESSING` state, a cryptographically unique fencing token (`processing_token` UUID) is generated and stored in `processed_events`.
2. **Lease Expiration:** An event in `PROCESSING` status is considered actively leased for the duration configured by `idempotency.processing-lease-duration` (default: 60s).
   - If a duplicate event arrives within the lease window, it is rejected with `EventProcessingInProgressException` and the Kafka offset is **NOT** acknowledged (triggering broker redelivery).
3. **Atomic Stale Reclaim:** If the lease has expired (`updated_at < :staleThreshold`), another worker reclaims the event atomically via:
   ```sql
   UPDATE processed_events
   SET status = 'PROCESSING',
       processing_token = :newToken,
       retry_count = retry_count + 1,
       updated_at = :now
   WHERE event_id = :eventId
     AND (status IN ('SUBMITTED', 'RECEIVED', 'FAILED')
          OR (status = 'PROCESSING' AND updated_at < :staleThreshold))
   ```
   PostgreSQL row locks serialize concurrent reclaimers. Exactly one reclaimer wins, installs its `:newToken`, and resets `updated_at`. Competing reclaimers see `updated_at >= :staleThreshold` and are safely rejected as `ACTIVE_PROCESSING`.
4. **Ownership Validation on Completion / Failure:**
   ```sql
   UPDATE processed_events
   SET status = :newStatus,
       processed_at = :processedAt,
       error_message = :errorMessage,
       updated_at = :now
   WHERE event_id = :eventId
     AND status = :expectedStatus
     AND processing_token = :processingToken
   ```
   When a worker finishes, its update checks `processing_token = :processingToken`. If the worker was reclaimed during a pause/GC stall, its token is stale:
   - The query updates **0 rows**.
   - `markCompleted` returns `false` and throws `StaleProcessingLeaseException`.
   - The stale worker cannot overwrite the newer worker's `PROCESSING`, `FAILED`, or `COMPLETED` state.
5. **External Side Effect Consideration:**
   Database fencing tokens strictly protect state mutations within the relational database (e.g., `payments` and `processed_events` in the same ACID transaction). A database fencing token alone cannot magically make un-fenced, non-transactional external side effects (such as remote REST API calls to third-party payment gateways without idempotency keys) exactly-once if a worker pauses after calling the external API but before committing the database transaction. Exactly-once guarantees require business processing to be transactional within PostgreSQL or downstream systems to support idempotency/fencing keys.

---

### 6. Redis Fast-Path Conventions

- **Role:** Redis provides a fast-path optimization for previously completed events (`GET event:idempotency:{eventId}`).
- **Authoritative Invariant:** Redis is NOT the authoritative idempotency mechanism; PostgreSQL remains the durable source of truth.
- **Outage Resilience:** Redis loss, eviction, restart, or connection failure must never compromise idempotency. If Redis operations throw an exception or return a cache miss, the system transparently falls back to PostgreSQL.

---

### 7. Error Response Conventions (RFC 7807)

Errors return standard JSON adhering to RFC 7807 Problem Details:

```json
{
  "type": "https://api.eventprocessor.example.com/errors/validation-failed",
  "title": "Validation Failed",
  "status": 400,
  "detail": "Invalid request parameters in request body",
  "instance": "/api/events",
  "timestamp": "2026-09-07T12:00:00.000Z",
  "errors": [
    {
      "field": "eventId",
      "rejectedValue": "",
      "message": "eventId must not be blank"
    }
  ]
}
```

Internal exception details and stack traces are suppressed from client responses and logged solely in server logs.

---

### 8. Logging & Observability Conventions

#### 8.1 Mapped Diagnostic Context (MDC)
Every log statement within the event processing pipeline must include:
- `traceId`: Unique identifier for the HTTP request or Kafka message context.
- `eventId`: The idempotency key of the event being handled.
- `eventType`: The business classification (`PAYMENT_COMPLETED`, `ORDER_CREATED`).

#### 8.2 Standard Log Format
```
2026-09-07 12:00:01.125 [kafka-consumer-1] INFO  c.e.e.c.KafkaEventConsumer [traceId=tr-8812, eventId=EVT-1001-A, eventType=PAYMENT_COMPLETED] - Duplicate event detected via Redis fast-path. Skipping business execution.
```

---

### 9. Frontend Policy & UI Notice

This project is strictly a backend event-processing service. 
- No frontend code or web UI in initial releases.
- If an admin monitoring UI or DLQ explorer is introduced later, its visual design must be specified in a separate design document before any implementation begins.
