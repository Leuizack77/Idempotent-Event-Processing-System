# Project Memory & Progress Tracking
## Idempotent Event Processing System

---

### 1. Project State Overview
- **Current Phase:** Phase 3 (Completed)
- **Next Phase Awaiting Approval:** Phase 4 (Kafka producer and consumer)
- **Status:** Phase 3 REST Ingestion API implemented and verified with 15/15 passing tests across all layers.

---

### 2. Phase 3 Implementation Summary

#### What Was Implemented
1. **Request/Response DTO Layer:**
   - `EventRequestDto.java`: Record with Bean Validation enforcing `@NotBlank` and `@Size(max = 128)` on `eventId`, `@NotBlank` on `eventType`, and `@NotEmpty` on `payload`.
   - `EventResponseDto.java`: Record returning `eventId`, `status` (`SUBMITTED`), `message`, and `submittedAt` timestamp.
   - `FieldErrorDto.java`: Record providing structured field error details (`field`, `rejectedValue`, `message`).
   - `ErrorResponseDto.java`: Record providing standard RFC 7807 Problem Details response (`type`, `title`, `status`, `detail`, `instance`, `timestamp`, `errors`).
2. **Exception Handling Layer:**
   - `GlobalExceptionHandler.java`: Annotated with `@RestControllerAdvice` centralizing error handling for:
     - `MethodArgumentNotValidException` -> 400 Bad Request with field-level RFC 7807 problem details.
     - `InvalidEventException` -> 400 Bad Request with descriptive message (e.g., unsupported event type, missing payload fields).
     - `DuplicateEventException` -> 409 Conflict when eventId already exists.
     - `HttpMessageNotReadableException` -> 400 Bad Request on malformed JSON bodies.
     - `Exception` -> 500 Internal Server Error (sanitized, stack traces strictly suppressed).
3. **Service Layer:**
   - `EventIngestionService.java` & `EventIngestionServiceImpl.java`:
     - Validates supported event types (`PAYMENT_COMPLETED`, `ORDER_CREATED`).
     - Validates payload structures per event type (ensuring required fields exist and are non-blank).
     - Checks for duplicate `eventId` against PostgreSQL.
     - Persists a `ProcessedEventEntity` with durable status `EventStatus.SUBMITTED`.
     - Strictly omits Kafka publishing (deferred to Phase 4).
4. **REST Controller Layer:**
   - `EventIngestionController.java`:
     - Exposes `POST /api/events`.
     - Enforces `@Valid` request body parsing.
     - Delegates directly to `EventIngestionService`.
     - Returns HTTP `202 Accepted` with `EventResponseDto`.
5. **Testing Suite:**
   - `EventIngestionControllerTest.java` running against real PostgreSQL via Testcontainers:
     - Test 1: Valid `PAYMENT_COMPLETED` event -> 202 Accepted, status `SUBMITTED`, persisted in DB.
     - Test 2: Missing `eventId` -> 400 Bad Request, RFC 7807 problem details.
     - Test 3: Blank `eventId` -> 400 Bad Request.
     - Test 4: `eventId` over 128 characters -> 400 Bad Request.
     - Test 5: Missing `eventType` -> 400 Bad Request.
     - Test 6: Unsupported `eventType` (`USER_DELETED`) -> 400 Bad Request.
     - Test 7: Missing or empty payload -> 400 Bad Request.
     - Test 8: Valid `ORDER_CREATED` event -> 202 Accepted, status `SUBMITTED`, persisted in DB.
     - Test 9: Duplicate `eventId` submission -> 409 Conflict.

---

### 3. Files Created & Modified

#### Files Created
- [`src/main/java/com/example/eventprocessor/dto/request/EventRequestDto.java`](file:///e:/New%20folder/src/main/java/com/example/eventprocessor/dto/request/EventRequestDto.java)
- [`src/main/java/com/example/eventprocessor/dto/response/EventResponseDto.java`](file:///e:/New%20folder/src/main/java/com/example/eventprocessor/dto/response/EventResponseDto.java)
- [`src/main/java/com/example/eventprocessor/dto/response/FieldErrorDto.java`](file:///e:/New%20folder/src/main/java/com/example/eventprocessor/dto/response/FieldErrorDto.java)
- [`src/main/java/com/example/eventprocessor/dto/response/ErrorResponseDto.java`](file:///e:/New%20folder/src/main/java/com/example/eventprocessor/dto/response/ErrorResponseDto.java)
- [`src/main/java/com/example/eventprocessor/exception/InvalidEventException.java`](file:///e:/New%20folder/src/main/java/com/example/eventprocessor/exception/InvalidEventException.java)
- [`src/main/java/com/example/eventprocessor/exception/DuplicateEventException.java`](file:///e:/New%20folder/src/main/java/com/example/eventprocessor/exception/DuplicateEventException.java)
- [`src/main/java/com/example/eventprocessor/exception/GlobalExceptionHandler.java`](file:///e:/New%20folder/src/main/java/com/example/eventprocessor/exception/GlobalExceptionHandler.java)
- [`src/main/java/com/example/eventprocessor/service/EventIngestionService.java`](file:///e:/New%20folder/src/main/java/com/example/eventprocessor/service/EventIngestionService.java)
- [`src/main/java/com/example/eventprocessor/service/EventIngestionServiceImpl.java`](file:///e:/New%20folder/src/main/java/com/example/eventprocessor/service/EventIngestionServiceImpl.java)
- [`src/main/java/com/example/eventprocessor/controller/EventIngestionController.java`](file:///e:/New%20folder/src/main/java/com/example/eventprocessor/controller/EventIngestionController.java)
- [`src/test/java/com/example/eventprocessor/controller/EventIngestionControllerTest.java`](file:///e:/New%20folder/src/test/java/com/example/eventprocessor/controller/EventIngestionControllerTest.java)

#### Files Modified
- [`docs/memory.md`](file:///e:/New%20folder/docs/memory.md) — Updated tracking state with Phase 3 completion.

---

### 4. Important Technical Decisions

1. **DTO & Entity Separation:**
   - Implemented immutable Java `record` types for all REST contracts. No JPA entities are exposed through REST endpoints.
2. **RFC 7807 Compliance:**
   - Validation and business errors return standardized Problem Details (`type`, `title`, `status`, `detail`, `instance`, `timestamp`, `errors`).
   - Internal stack traces are strictly hidden from clients.
3. **Strict Scope Discipline:**
   - Kafka publishing and message queues were intentionally omitted (scheduled for Phase 4).
   - Event processing and deduplication algorithms were intentionally omitted (scheduled for Phase 5 & 6).
   - Only documented event types (`PAYMENT_COMPLETED`, `ORDER_CREATED`) are accepted.

---

### 5. Tests Executed & Exact Maven Results

- **Commands Executed:**
  - `mvn clean compile`
  - `mvn test`
- **Results:**
  ```text
  [INFO] Running com.example.eventprocessor.controller.EventIngestionControllerTest
  [INFO] Tests run: 9, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 27.68 s -- in com.example.eventprocessor.controller.EventIngestionControllerTest
  [INFO] Running com.example.eventprocessor.EventProcessorApplicationTests
  [INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.146 s -- in com.example.eventprocessor.EventProcessorApplicationTests
  [INFO] Running com.example.eventprocessor.repository.PostgresPersistenceIntegrationTest
  [INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 2.404 s -- in com.example.eventprocessor.repository.PostgresPersistenceIntegrationTest
  [INFO] 
  [INFO] Results:
  [INFO] Tests run: 15, Failures: 0, Errors: 0, Skipped: 0
  [INFO] ------------------------------------------------------------------------
  [INFO] BUILD SUCCESS
  [INFO] Total time:  38.705 s
  ```

---

### 6. Known Limitations
- Events are persisted as `SUBMITTED` in PostgreSQL, but not yet published to Apache Kafka (scheduled for Phase 4).
- No background consumers or deduplication workflows exist yet (scheduled for Phase 4 and 5).

---

### 7. Phase Progression Status
- **Completed Phases:** Phase 1, Phase 2, Phase 3.
- **Current Status:** Awaiting user approval to proceed to Phase 4: Kafka Producer and Consumer.
