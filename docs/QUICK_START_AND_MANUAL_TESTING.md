# Complete Beginner Quick Start & Manual Testing Guide
## Idempotent Event Processing System

---

## 1. Project Overview

### What is the Idempotent Event Processing System?
The **Idempotent Event Processing System** is a production-grade backend service built to reliably ingest, queue, and process business events (such as customer payments and purchase orders) without **ever processing the same event twice**.

### What problem does it solve?
In modern distributed systems, computers communicate over networks. Networks are inherently unreliable:
* Packets get dropped.
* HTTP requests timeout.
* Senders retry sending the same message when they don't receive an acknowledgment in time.
* Message brokers like Kafka guarantee **at-least-once delivery**, which means a consumer can occasionally receive duplicate copies of the same event.

Without **idempotency**, if a customer clicks "Pay $150" and network latency causes a retry, the system might charge the customer $150 twice ($300 total). 

**Idempotency** guarantees that no matter how many times an identical event is submitted or redelivered, the underlying business mutation (e.g. creating a payment or creating an order) is executed **exactly once**, and subsequent attempts are safely intercepted.

### What does each component do?
* **Spring Boot 3 (Java 21):** The core application engine. It provides the REST ingestion API (`POST /api/events`), orchestrates idempotency checks, runs Kafka producers and consumers, executes business logic, and exposes health/metrics endpoints.
* **PostgreSQL 16:** The **authoritative source of truth**. It stores the event processing states (`SUBMITTED`, `PROCESSING`, `COMPLETED`, `FAILED`) and application data (`payments`, `orders`). It enforces unique constraints (`uq_processed_events_event_id`), lease expiration thresholds, and guarantees ACID atomic transaction commits.
* **Apache Kafka 3.8.0 (KRaft mode):** The distributed message broker. It decouples HTTP ingestion from business processing. The REST API quickly accepts events, writes a `SUBMITTED` record, publishes to the `events` Kafka topic, and returns `HTTP 202 Accepted`. Kafka then delivers events to consumers with partition-level ordering. If processing permanently fails, messages are routed to `events.dlq` (Dead Letter Queue).
* **Redis 7.2:** A **non-authoritative fast-path cache**. When an event is successfully marked `COMPLETED` in PostgreSQL, a lightweight key (`event-processing:{eventId}`) is written to Redis with a 24-hour TTL. Future duplicate attempts can be intercepted in sub-millisecond time without querying PostgreSQL. If Redis crashes or is unavailable, the application **fails open** and queries PostgreSQL directly.
* **Docker Compose:** The container orchestrator. Instead of manually installing and configuring PostgreSQL, Kafka, and Redis on your machine, Docker Compose starts all four components (PostgreSQL, Kafka, Redis, and the Spring Boot App) with a single command, automatically wiring up internal networks, persistent storage, and dependency health checks.

---

## 2. Architecture & Event Flow

### Text Architecture Diagram

```text
                  +-------------------------------------------------------------+
                  |                      CLIENT (Browser / Postman / curl)      |
                  +-------------------------------------------------------------+
                                                 |
                                                 | 1. POST /api/events (JSON payload)
                                                 v
                  +-------------------------------------------------------------+
                  |            SPRING BOOT INGESTION CONTROLLER                 |
                  |                (EventIngestionController)                   |
                  +-------------------------------------------------------------+
                                    |                           |
                2. Check unique ID  |                           | 4. Publish message
                & record SUBMITTED  |                           |    (Key = eventId)
                                    v                           v
                      +-------------------+           +-------------------+
                      |   POSTGRESQL 16   |           |    KAFKA TOPIC    |
                      | (processed_events)|           |     ("events")    |
                      +-------------------+           +-------------------+
                                                                |
                                                                | 5. Consume record
                                                                v
                                              +-----------------------------------+
                                              |        KAFKA EVENT CONSUMER       |
                                              |       (KafkaEventConsumer)        |
                                              +-----------------------------------+
                                                                |
                                                                v
                                              +-----------------------------------+
                                              |        IDEMPOTENCY ENGINE         |
                                              |      (IdempotencyEngineImpl)      |
                                              +-----------------------------------+
                                                    /           |           \
                                                   /            |            \
                          6. Fast-path check      /   7. Atomic |             \ 8. If fatal
                                                 /   reservation|              \   failure
                                                v               v               v
                                      +-------------+   +---------------+   +---------------+
                                      |   REDIS     |   |  POSTGRESQL   |   |   KAFKA DLQ   |
                                      |  (Cache)    |   | (Lease Token) |   | ("events.dlq")|
                                      +-------------+   +---------------+   +---------------+
                                                                |
                                                       9. Lease acquired:
                                                          Execute Business Logic
                                                          within @Transactional
                                                                |
                                                                v
                                              +-----------------------------------+
                                              |    BUSINESS EVENT PROCESSORS      |
                                              | (PaymentEventProcessor / Orders)  |
                                              +-----------------------------------+
                                                                |
                                                                | 10. Persist entity
                                                                |     & mark COMPLETED
                                                                v
                                              +-----------------------------------+
                                              |           POSTGRESQL              |
                                              | (payments / orders / COMPLETED)   |
                                              +-----------------------------------+
                                                                |
                                                                | 11. Write cache & Ack
                                                                v
                                              +-----------------------------------+
                                              | REDIS (TTL: 24h) + KAFKA ACK      |
                                              +-----------------------------------+
```

### Step-by-Step Flow
1. **Client Submission:** The client sends an HTTP `POST` request to `/api/events` containing an `eventId`, `eventType`, and `payload`.
2. **Ingestion Validation:** `EventIngestionServiceImpl` verifies that the `eventType` is supported (`PAYMENT_COMPLETED` or `ORDER_CREATED`) and all required payload fields exist.
3. **Pre-Registration:** If valid and new, the event is saved to PostgreSQL in `processed_events` with status `SUBMITTED`. If the `eventId` already exists, it is immediately rejected with `HTTP 409 Conflict`.
4. **Kafka Publishing:** The event is wrapped in an `EventEnvelope` and published to the Kafka topic `events` using `eventId` as the message key.
5. **Immediate Client Response:** The REST API returns `HTTP 202 Accepted` to the client. The client does not wait for database business operations to complete.
6. **Kafka Consumption:** `KafkaEventConsumer` pulls the record from Kafka.
7. **Fast-Path Cache Lookup:** `IdempotencyEngineImpl` queries Redis for `event-processing:{eventId}`. If found and `COMPLETED`, processing stops and offset is acknowledged (`SKIPPED_DUPLICATE`).
8. **Durable Reservation (PostgreSQL):** If not in cache, `IdempotencyServiceImpl.tryReserve()` runs an isolated transaction (`REQUIRES_NEW`). It transitions the status to `PROCESSING` and assigns a unique UUID `processing_token`. If the event is already `COMPLETED`, it populates Redis and returns `SKIPPED_DUPLICATE`. If another worker is actively processing it within its 60-second lease window, the duplicate attempt is rejected.
9. **Atomic Business Execution:** With the reservation lease held, `TransactionalBusinessExecutor` executes the processor (`PaymentEventProcessor` or `OrderEventProcessor`). It inserts the business entity (e.g. into `payments`) and updates the `processed_events` record to `COMPLETED` **within the exact same database transaction**.
10. **Post-Commit Cache & Acknowledgment:** Once committed, Redis is populated with `COMPLETED` (TTL 24 hours), and the Kafka consumer issues a manual acknowledgment (`ackMode: manual_immediate`) committing the Kafka offset.
11. **Error / DLQ Routing:** If a non-retryable exception occurs (e.g. negative payment amount) or retries are exhausted, the record is published to `events.dlq` with failure diagnostic headers, the database status is marked `FAILED`, and the offset is committed to avoid blocking the queue.

---

## 3. Technology Stack

| Technology | Version | Why It Is Used in This Project |
| :--- | :--- | :--- |
| **Java** | 21 (LTS) | Modern Java runtime utilizing records, pattern matching, and strong typing. |
| **Spring Boot** | 3.3.4 | Application framework providing REST controllers, dependency injection, and configuration. |
| **PostgreSQL** | 16 (Alpine) | Authoritative database providing ACID transactions, unique constraints, and row-level locking. |
| **Apache Kafka** | 3.8.0 (KRaft) | High-throughput distributed message broker running without Zookeeper. |
| **Redis** | 7.2 (Alpine) | In-memory key-value cache for sub-millisecond idempotency deduplication fast-path. |
| **Flyway** | 10.x | Version-controlled database migrations applied automatically upon startup. |
| **Spring Data JPA / Hibernate** | 6.5.x | Object-relational mapping (ORM) for domain entities (`ProcessedEventEntity`, `PaymentEntity`, `OrderEntity`). |
| **Spring Kafka** | 3.2.x | Kafka template, consumer listener containers, error handlers, and dead-letter publishing recoverers. |
| **Micrometer & Actuator** | 3.3.x | Production observability, metrics collection (`/actuator/prometheus`), and container health checks (`/actuator/health`). |
| **Docker & Docker Compose** | v2 / Compose v2 | Container packaging and multi-container orchestration. |

---

## 4. Prerequisites

### Host Machine Requirements
To run this project using Docker Compose, you only need:
1. **Git:** To clone the repository.
2. **Docker Desktop** (or Docker Engine on Linux): To build and run containers.
3. **PowerShell** (Windows) or **Terminal** (macOS/Linux): To execute verification commands.

### How to Verify Host Tools

Open PowerShell or your terminal and verify:

```powershell
git --version
docker --version
docker compose version
```

Expected output:
* `git version 2.x.x`
* `Docker version 20.x.x` (or newer, e.g., `27.x.x` / `29.x.x`)
* `Docker Compose version v2.x.x`

> [!NOTE]
> **Do I need Java or Maven installed on my host computer?**
> **No.** The `Dockerfile` uses a multi-stage build: it runs Maven and compiles Java 21 **inside a container**. You do not need Java or Maven installed on your host to run or test the application. (If you want to run `mvn clean test` directly on your host machine outside of Docker, Java 21 and Maven 3.9+ are required).

---

## 5. Cloning the Project

Open PowerShell and navigate to the directory where you want to keep the project:

```powershell
cd E:\
git clone https://github.com/Leuizack77/Idempotent-Event-Processing-System.git
cd "Idempotent-Event-Processing-System"
```

Verify you are on the `main` branch:

```powershell
git branch --show-current
```

Expected output:
```text
main
```

---

## 6. Environment Configuration

The repository includes an environment template file: `.env.example`.

### How to create your `.env` file

In PowerShell, copy `.env.example` to `.env`:

```powershell
Copy-Item .env.example .env
```

### Understanding Environment Variables

| Variable | Default Value | Purpose | Scope |
| :--- | :--- | :--- | :--- |
| `SPRING_PROFILES_ACTIVE` | `docker` | Activates `application-docker.yml` configuration. | Container |
| `DB_HOST` | `postgres` | Hostname of the PostgreSQL container inside Docker network. | Docker internal |
| `DB_PORT` | `5432` | Internal PostgreSQL port. | Docker internal |
| `DB_NAME` | `event_processor` | PostgreSQL database name. | Docker internal |
| `DB_USERNAME` | `postgres` | PostgreSQL database user. | Docker internal |
| `DB_PASSWORD` | `postgres` | PostgreSQL database password. | Docker internal |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker endpoint inside Docker network. | Docker internal |
| `REDIS_HOST` | `redis` | Redis container hostname inside Docker network. | Docker internal |
| `REDIS_PORT` | `6379` | Internal Redis port. | Docker internal |
| `IDEMPOTENCY_LEASE_DURATION` | `60s` | Maximum time an event remains in `PROCESSING` before considered stale. | Container |
| `IDEMPOTENCY_CACHE_TTL` | `24h` | Time-to-live for completed event keys in Redis. | Container |
| `HOST_APP_PORT` | `8080` | Port mapped on your host machine for the REST API. | Host accessible |
| `HOST_DB_PORT` | `5432` | Port mapped on your host machine to access PostgreSQL directly. | Host accessible |
| `HOST_KAFKA_PORT` | `9094` | Port mapped on your host machine to access Kafka directly. | Host accessible |
| `HOST_REDIS_PORT` | `6379` | Port mapped on your host machine to access Redis directly. | Host accessible |

> [!TIP]
> **Is `.env` mandatory?**
> No. In `docker-compose.yml`, every variable has a fallback default (e.g. `${DB_NAME:-event_processor}`). Even if you don't create `.env`, `docker compose up -d` will start with these exact defaults.

---

## 7. Understanding the Docker Compose Services

When you run Docker Compose, four services are created on a private bridge network called `event-network`:

```text
+-----------------------------------------------------------------------------------------+
|                                     DOCKER NETWORK                                      |
|                                                                                         |
|  +--------------------------+                         +------------------------------+  |
|  | postgres                 |                         | kafka                        |  |
|  | Container:               |                         | Container:                   |  |
|  |   event-processor-postgres                         |   event-processor-kafka      |  |
|  | Image: postgres:16-alpine|                         | Image: apache/kafka:3.8.0    |  |
|  | Internal: 5432           |                         | Internal: 9092, 9093         |  |
|  | External: 5432           |                         | External: 9094               |  |
|  +--------------------------+                         +------------------------------+  |
|               ^                                                      ^                  |
|               | (healthy)                                            | (healthy)        |
|               +-------------------+             +--------------------+                  |
|                                   |             |                                       |
|                               +----------------------+                                  |
|                               | app                  |                                  |
|                               | Container:           |                                  |
|                               |   event-processor-app|                                  |
|                               | Internal: 8080       |                                  |
|                               | External: 8080       |                                  |
|                               +----------------------+                                  |
|                                          |                                              |
|                                          v (healthy)                                    |
|                               +----------------------+                                  |
|                               | redis                |                                  |
|                               | Container:           |                                  |
|                               |  event-processor-redis                                  |
|                               | Image: redis:7.2-alpi|                                  |
|                               | Internal/Ext: 6379   |                                  |
|                               +----------------------+                                  |
+-----------------------------------------------------------------------------------------+
```

1. **`postgres` (`event-processor-postgres`)**:
   * Image: `postgres:16-alpine`
   * Host Port: `5432`
   * Healthcheck: `pg_isready -U postgres -d event_processor`
   * Volume: `postgres_data` (keeps your database data safe even when containers stop).
2. **`kafka` (`event-processor-kafka`)**:
   * Image: `apache/kafka:3.8.0`
   * Host Port: `9094` (internal listeners: `9092` plaintext, `9093` controller)
   * Healthcheck: Queries cluster metadata using `kafka-cluster.sh cluster-id`.
3. **`redis` (`event-processor-redis`)**:
   * Image: `redis:7.2-alpine`
   * Host Port: `6379`
   * Healthcheck: `redis-cli ping`
4. **`app` (`event-processor-app`)**:
   * Image: Built from local `Dockerfile` (multi-stage Java 21 Alpine).
   * Host Port: `8080`
   * Dependencies: Waits until `postgres`, `kafka`, and `redis` report **healthy** before starting.
   * Healthcheck: `wget -q --spider http://localhost:8080/actuator/health`

---

## 8. First Startup

Make sure your Docker Desktop engine is running. Then in PowerShell, run:

```powershell
docker compose up -d
```

### Inspect Container Health

Check the status of all four containers:

```powershell
docker compose ps
```

### Expected Output

During the first 15–25 seconds, the `app` container will display `(health: starting)`:

```text
NAME                       IMAGE                                 SERVICE    STATUS
event-processor-app        idempotenteventprocessingsystem-app   app        Up 10 seconds (health: starting)
event-processor-kafka      apache/kafka:3.8.0                    kafka      Up 20 seconds (healthy)
event-processor-postgres   postgres:16-alpine                    postgres   Up 20 seconds (healthy)
event-processor-redis      redis:7.2-alpine                      redis      Up 20 seconds (healthy)
```

> [!IMPORTANT]
> **Why does `app` show `(health: starting)`?**
> The application Dockerfile sets a start period of 30 seconds (`--start-period=30s`). This gives Spring Boot enough time to start the JVM, run Flyway migrations, establish database connection pools, and register with Kafka before Docker starts checking if it's healthy.

After 25–35 seconds, run `docker compose ps` again:

```text
NAME                       IMAGE                                 SERVICE    STATUS
event-processor-app        idempotenteventprocessingsystem-app   app        Up 30 seconds (healthy)
event-processor-kafka      apache/kafka:3.8.0                    kafka      Up 35 seconds (healthy)
event-processor-postgres   postgres:16-alpine                    postgres   Up 35 seconds (healthy)
event-processor-redis      redis:7.2-alpine                      redis      Up 35 seconds (healthy)
```

All 4 services should now display **`(healthy)`**.

---

## 9. Verifying System Health

To confirm the Spring Boot application is ready to accept requests, query the Spring Boot Actuator health endpoint:

```powershell
curl.exe -s http://localhost:8080/actuator/health
```
*(Or in PowerShell)*:
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/actuator/health" -Method Get | ConvertTo-Json
```

### Expected Output:
```json
{
  "status": "UP",
  "groups": [
    "liveness",
    "readiness"
  ]
}
```

---

## 10. Viewing Application Logs

Monitoring container logs is the best way to see the event pipeline in action.

### View all logs
```powershell
docker compose logs
```

### View only application logs
```powershell
docker compose logs app
```

### Follow live logs (press Ctrl+C to exit)
```powershell
docker compose logs -f app
```

### View logs from the last 10 minutes
```powershell
docker compose logs app --since=10m
```

### Search logs for a specific event ID (PowerShell)
```powershell
docker compose logs app | Select-String "EVT-PAY-GUIDE-001"
```

### Search logs for errors or failures (PowerShell)
```powershell
docker compose logs app --since=10m | Select-String "ERROR|Exception|FAILED"
```

### What Key Log Lines Mean
* `Event successfully accepted and persisted as SUBMITTED`: The REST controller validated the payload and pre-registered the event in PostgreSQL.
* `Publishing event to Kafka`: The application sent the message to the `events` Kafka topic.
* `Kafka record received`: The background consumer (`KafkaEventConsumer`) picked up the message.
* `Successfully reserved/reclaimed eventId=... to PROCESSING with token=...`: The idempotency engine acquired the PostgreSQL execution lease.
* `Executing transactional business processing`: Business logic (e.g. creating payment) is executing inside an active database transaction.
* `Business execution and COMPLETED state successfully committed`: Business row was written and status changed to `COMPLETED` atomically.
* `Redis cache populated`: Key was written to Redis with 24-hour TTL for future fast-path skipping.
* `Kafka offset acknowledged`: Consumer committed offset to Kafka.
* `Redis cache HIT: ... Skipping PostgreSQL reservation`: Fast-path duplicate interception.

---

## 11. Event Status Lifecycle

Every event moves through explicit, well-defined states stored in the `processed_events` table in PostgreSQL:

```text
                  [ REST POST /api/events ]
                              |
                              v
                        +-----------+
                        | SUBMITTED |  (Event accepted via HTTP, written to DB & Kafka)
                        +-----------+
                              |
                              v
                       +------------+
                       | PROCESSING |  (Consumer reserved event with UUID token & 60s lease)
                       +------------+
                             / \
           (Business Success)   (Fatal error or retries exhausted)
                           /     \
                          v       v
                   +-----------+ +--------+
                   | COMPLETED | | FAILED |  (Terminal states)
                   +-----------+ +--------+
```

### Status Descriptions
* **`SUBMITTED`:** The REST request was valid. The record was written to PostgreSQL and dispatched to Kafka.
* **`PROCESSING`:** The Kafka consumer received the event and acquired an atomic reservation lease with a unique `processing_token`. If the server crashes during processing, the lease expires after 60 seconds (`IDEMPOTENCY_LEASE_DURATION`), allowing another consumer to reclaim it.
* **`COMPLETED`:** The business mutation (payment or order) was persisted and the event was finalized. This is a terminal state.
* **`FAILED`:** A non-retryable exception occurred (e.g. invalid negative amount) or maximum retries were exhausted. The event was forwarded to `events.dlq` for operator inspection. This is a terminal state.

---

## 12. Manual Test: Payment Completed (`PAYMENT_COMPLETED`)

Let's test processing a successful payment.

### Step 1: Submit Event via REST API

In PowerShell, send a `POST` request to `http://localhost:8080/api/events`:

```powershell
$paymentEvent = @'
{
  "eventId": "EVT-PAY-GUIDE-001",
  "eventType": "PAYMENT_COMPLETED",
  "payload": {
    "paymentId": "PAY-GUIDE-001",
    "userId": "USER-GUIDE-100",
    "amount": 250.00,
    "currency": "USD"
  }
}
'@

curl.exe -s -i -X POST http://localhost:8080/api/events `
  -H "Content-Type: application/json" `
  -d $paymentEvent
```

### Step 2: Expected HTTP Response
```http
HTTP/1.1 202 
X-Correlation-ID: tr-xxxxxxxx
X-Trace-ID: tr-xxxxxxxx
Content-Type: application/json

{
  "eventId": "EVT-PAY-GUIDE-001",
  "status": "SUBMITTED",
  "message": "Event accepted for asynchronous processing",
  "submittedAt": "2026-09-13T..."
}
```

### Step 3: Verify in PostgreSQL

Wait 1–2 seconds for asynchronous Kafka processing to complete, then inspect the PostgreSQL database using `docker compose exec`:

```powershell
docker compose exec postgres psql -U postgres -d event_processor -c "SELECT event_id, event_type, status, retry_count, processing_token FROM processed_events WHERE event_id = 'EVT-PAY-GUIDE-001';"
docker compose exec postgres psql -U postgres -d event_processor -c "SELECT payment_id, user_id, amount, currency FROM payments WHERE payment_id = 'PAY-GUIDE-001';"
```

### Expected Database Output:
```text
      event_id      |    event_type     |  status   | retry_count |           processing_token           
--------------------+-------------------+-----------+-------------+--------------------------------------
 EVT-PAY-GUIDE-001  | PAYMENT_COMPLETED | COMPLETED |           0 | 4a3f7b2e-....
(1 row)

  payment_id   |    user_id     |  amount  | currency 
---------------+----------------+----------+----------
 PAY-GUIDE-001 | USER-GUIDE-100 | 250.0000 | USD
(1 row)
```

### Step 4: Verify in Redis Cache
Check that the event was added to Redis:

```powershell
docker compose exec redis redis-cli get event-processing:EVT-PAY-GUIDE-001
docker compose exec redis redis-cli ttl event-processing:EVT-PAY-GUIDE-001
```

### Expected Redis Output:
```text
COMPLETED
86395
```
*(The TTL shows remaining seconds out of 86,400 seconds = 24 hours).*

---

## 13. Manual Test: Order Created (`ORDER_CREATED`)

Now let's test the second supported event type: `ORDER_CREATED`.

### Step 1: Submit Event via REST API

In PowerShell:

```powershell
$orderEvent = @'
{
  "eventId": "EVT-ORD-GUIDE-001",
  "eventType": "ORDER_CREATED",
  "payload": {
    "orderId": "ORD-GUIDE-001",
    "customerId": "CUST-GUIDE-200",
    "totalAmount": 99.50,
    "itemCount": 2
  }
}
'@

curl.exe -s -i -X POST http://localhost:8080/api/events `
  -H "Content-Type: application/json" `
  -d $orderEvent
```

### Step 2: Expected HTTP Response
```http
HTTP/1.1 202 
Content-Type: application/json

{
  "eventId": "EVT-ORD-GUIDE-001",
  "status": "SUBMITTED",
  "message": "Event accepted for asynchronous processing",
  "submittedAt": "2026-09-13T..."
}
```

### Step 3: Verify in PostgreSQL

```powershell
docker compose exec postgres psql -U postgres -d event_processor -c "SELECT event_id, event_type, status FROM processed_events WHERE event_id = 'EVT-ORD-GUIDE-001';"
docker compose exec postgres psql -U postgres -d event_processor -c "SELECT order_id, customer_id, total_amount, itemCount FROM orders WHERE order_id = 'ORD-GUIDE-001';"
```

### Expected Database Output:
```text
      event_id     |  event_type   |  status   
-------------------+---------------+-----------
 EVT-ORD-GUIDE-001 | ORDER_CREATED | COMPLETED
(1 row)

   order_id    |   customer_id  | total_amount | item_count 
---------------+----------------+--------------+------------
 ORD-GUIDE-001 | CUST-GUIDE-200 |      99.5000 |          2
(1 row)
```

---

## 14. Manual Test: Duplicate Event Interception

To see idempotency in action, resubmit the **exact same payment event** (`EVT-PAY-GUIDE-001`) that was completed in Section 12.

### Step 1: Resubmit Identical Event

```powershell
$duplicatePayment = @'
{
  "eventId": "EVT-PAY-GUIDE-001",
  "eventType": "PAYMENT_COMPLETED",
  "payload": {
    "paymentId": "PAY-GUIDE-001",
    "userId": "USER-GUIDE-100",
    "amount": 250.00,
    "currency": "USD"
  }
}
'@

curl.exe -s -i -X POST http://localhost:8080/api/events `
  -H "Content-Type: application/json" `
  -d $duplicatePayment
```

### Step 2: Expected HTTP Response (HTTP 409 Conflict)

The system detects that `EVT-PAY-GUIDE-001` already exists and rejects it immediately:

```http
HTTP/1.1 409 
Content-Type: application/json

{
  "type": "https://api.eventprocessor.example.com/errors/duplicate-event",
  "title": "Duplicate Event",
  "status": 409,
  "detail": "Event with eventId 'EVT-PAY-GUIDE-001' already exists",
  "instance": "/api/events",
  "timestamp": "2026-09-13T..."
}
```

### Step 3: Verify Zero Duplicate Side-Effects

Verify that the `payments` table still contains **only 1 record**:

```powershell
docker compose exec postgres psql -U postgres -d event_processor -c "SELECT count(*) FROM payments WHERE payment_id = 'PAY-GUIDE-001';"
```

### Expected Output:
```text
 count 
-------
     1
(1 row)
```
The customer was NOT charged twice. The duplicate was intercepted cleanly!

---

## 15. Manual Test: Validation & Invalid Events

The REST ingestion layer validates events before anything is written to the database or Kafka.

### Test A: Unsupported Event Type
Try submitting an event type that the system does not recognize:

```powershell
$badType = @'
{
  "eventId": "EVT-INVALID-TYPE-001",
  "eventType": "USER_REGISTERED",
  "payload": {
    "username": "alice"
  }
}
'@

curl.exe -s -i -X POST http://localhost:8080/api/events `
  -H "Content-Type: application/json" `
  -d $badType
```

#### Expected Response: `HTTP 400 Bad Request`
```json
{
  "type": "https://api.eventprocessor.example.com/errors/invalid-event",
  "title": "Invalid Event",
  "status": 400,
  "detail": "Unsupported eventType: 'USER_REGISTERED'. Supported types: [PAYMENT_COMPLETED, ORDER_CREATED]",
  "instance": "/api/events"
}
```

### Test B: Missing Required Payload Field
Try submitting a payment missing the required `amount` field:

```powershell
$missingField = @'
{
  "eventId": "EVT-INVALID-PAY-001",
  "eventType": "PAYMENT_COMPLETED",
  "payload": {
    "paymentId": "PAY-MISSING-001",
    "userId": "USER-100",
    "currency": "USD"
  }
}
'@

curl.exe -s -i -X POST http://localhost:8080/api/events `
  -H "Content-Type: application/json" `
  -d $missingField
```

#### Expected Response: `HTTP 400 Bad Request`
```json
{
  "type": "https://api.eventprocessor.example.com/errors/invalid-event",
  "title": "Invalid Event",
  "status": 400,
  "detail": "Missing or blank required field in payload: 'amount'",
  "instance": "/api/events"
}
```

### Confirm Invalid Events Are Not Stored
Check PostgreSQL to confirm `EVT-INVALID-PAY-001` was completely rejected:

```powershell
docker compose exec postgres psql -U postgres -d event_processor -c "SELECT count(*) FROM processed_events WHERE event_id = 'EVT-INVALID-PAY-001';"
```
Output: `0` rows.

---

## 16. Kafka Retry Behavior

The application differentiates between **transient (retryable)** errors and **fatal (non-retryable)** errors:

### Retry Architecture
* **Retryable Exceptions:** Database deadlocks, temporary lock acquisition contention (`CannotAcquireLockException`), transient network hiccups.
* **Non-Retryable Exceptions:** Business logic validation errors, malformed payloads (`InvalidEventPayloadException`), duplicate business entities, JSON deserialization failures.
* **Retry Configuration (`application.yml`):**
  * Initial interval: `1000ms` (1 second)
  * Multiplier: `2.0` (exponential backoff: 1s, 2s, 4s...)
  * Max interval: `10000ms` (10 seconds)
  * Max attempts: `3`
* **How Retry Works in Memory & Database:**
  When a transient exception occurs, Spring Kafka's `DefaultErrorHandler` triggers the in-memory backoff loop. In PostgreSQL, `IdempotencyServiceImpl.prepareForRetry()` sets the entity's `updated_at` timestamp to an expired threshold, allowing the subsequent retry attempt to re-reserve the lease and increment `retry_count`.
  If the transient condition clears on attempt 2 or 3, the event transitions to `COMPLETED` and exactly one business mutation is committed.

---

## 17. Manual Test: Dead Letter Queue (DLQ) & Terminal `FAILED` State

What happens when an event has a **fatal business error** that can never succeed?
For example, submitting a payment with a **negative amount** (`-75.00`).

The REST API will accept it (because the field `amount` exists), but the Kafka consumer's `PaymentEventProcessor` will reject it as fatal.

### Step 1: Submit a Poison Pill Event

```powershell
$fatalEvent = @'
{
  "eventId": "EVT-DLQ-GUIDE-001",
  "eventType": "PAYMENT_COMPLETED",
  "payload": {
    "paymentId": "PAY-DLQ-001",
    "userId": "USER-DLQ-100",
    "amount": -75.00,
    "currency": "USD"
  }
}
'@

curl.exe -s -i -X POST http://localhost:8080/api/events `
  -H "Content-Type: application/json" `
  -d $fatalEvent
```

### Step 2: HTTP Response
The API accepts the event initially with `HTTP 202 Accepted`.

### Step 3: Observe Kafka Routing & PostgreSQL Status

Wait 2 seconds, then inspect PostgreSQL:

```powershell
docker compose exec postgres psql -U postgres -d event_processor -c "SELECT event_id, status, error_message, retry_count FROM processed_events WHERE event_id = 'EVT-DLQ-GUIDE-001';"
```

### Expected Output:
```text
      event_id     | status |               error_message                | retry_count 
-------------------+--------+--------------------------------------------+-------------
 EVT-DLQ-GUIDE-001 | FAILED | Field 'amount' must be positive, got: -75.0 |           0
(1 row)
```

Confirm no payment was inserted:
```powershell
docker compose exec postgres psql -U postgres -d event_processor -c "SELECT count(*) FROM payments WHERE payment_id = 'PAY-DLQ-001';"
```
Output: `0` rows.

### Step 4: Verify Message in Kafka DLQ

You can consume messages directly from the `events.dlq` topic using Kafka's command-line consumer inside the Kafka container:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic events.dlq --from-beginning --timeout-ms 5000
```

### Expected Output:
```json
{"eventId":"EVT-DLQ-GUIDE-001","eventType":"PAYMENT_COMPLETED","payload":{"amount":-75.0,"currency":"USD","paymentId":"PAY-DLQ-001","userId":"USER-DLQ-100"}}
Processed a total of 1 messages
```

> [!NOTE]
> **Why does kafka-console-consumer show a timeout?**
> When `--timeout-ms 5000` finishes, the script prints:
> `[2026-09-13 ...] ERROR Error processing message, terminating consumer process: org.apache.kafka.common.errors.TimeoutException: Timeout of 5000ms expired before successfully reading a message`
> This timeout message is completely normal and expected. It simply indicates that all available messages in the topic have been read and the console utility exited.

### Step 5: Check Application Logs for DLQ Confirmation

```powershell
docker compose logs app | Select-String "EVT-DLQ-GUIDE-001"
```

You will see:
* `Retries exhausted or fatal failure encountered. Publishing to DLQ: key=EVT-DLQ-GUIDE-001`
* `Durable status for eventId='EVT-DLQ-GUIDE-001' updated to FAILED upon DLQ routing`
* `DLQ Record received: topic=events.dlq, partition=..., offset=..., key=EVT-DLQ-GUIDE-001`

---

## 18. Redis Inspection & Fail-Open Resilience

Redis provides sub-millisecond deduplication, but **PostgreSQL remains the source of truth**.

### Inspect Redis Keys
```powershell
# List all event processing keys
docker compose exec redis redis-cli keys "event-processing:*"

# Read the value of an event
docker compose exec redis redis-cli get event-processing:EVT-PAY-GUIDE-001

# Inspect remaining Time-To-Live in seconds
docker compose exec redis redis-cli ttl event-processing:EVT-PAY-GUIDE-001
```

### Testing Fail-Open Resilience
What happens if Redis loses its data or a key expires?
Delete the key from Redis:

```powershell
docker compose exec redis redis-cli del event-processing:EVT-PAY-GUIDE-001
```

Now submit `EVT-PAY-GUIDE-001` again:
```powershell
curl.exe -s -i -X POST http://localhost:8080/api/events -H "Content-Type: application/json" -d $paymentEvent
```
Result: Still rejected with `HTTP 409 Conflict`!
Because Redis had a cache MISS, the application queried PostgreSQL, saw `COMPLETED`, backfilled Redis, and protected against the duplicate.

---

## 19. PostgreSQL Manual Inspection

You can connect to the interactive PostgreSQL command line (`psql`) at any time:

```powershell
docker compose exec postgres psql -U postgres -d event_processor
```

### Useful Inspection Commands

View table schema structure:
```sql
\d processed_events
\d payments
\d orders
```

View all events with their statuses and lease tokens:
```sql
SELECT id, event_id, event_type, status, retry_count, processing_token, created_at, processed_at
FROM processed_events
ORDER BY id ASC;
```

View all completed payments:
```sql
SELECT id, payment_id, user_id, amount, currency, created_at
FROM payments
ORDER BY id ASC;
```

View all completed orders:
```sql
SELECT id, order_id, customer_id, total_amount, item_count, created_at
FROM orders
ORDER BY id ASC;
```

Exit psql:
```sql
\q
```

---

## 20. Kafka Manual Inspection

Kafka runs inside the `event-processor-kafka` container. You can run Kafka admin tools directly inside the container.

### List All Topics
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
Expected output:
```text
events
events.dlq
events.retry
```

### Describe a Topic (Partitions & Replication)
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic events
```

### View Active Consumer Groups
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```
Expected output:
```text
event-processor-group
event-processor-dlq-group
```

### Check Consumer Lag
To verify whether consumers have processed all messages:
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group event-processor-group
```
`LAG` should be `0` when all events have been processed.

---

## 21. Observability: Tracing & Metrics

### Distributed Tracing (MDC Correlation ID)
Every request passing through `EventIngestionController` is assigned a correlation ID.
* If the caller provides `X-Correlation-ID` or `X-Trace-ID`, the system uses it.
* If omitted, the system generates a unique ID like `tr-xxxxxxxx`.
* This ID is logged across every step (`[traceId=..., eventId=..., eventType=...]`) and propagated across Kafka message headers.

Test sending your own custom correlation ID:

```powershell
$customTraceEvent = @'
{
  "eventId": "EVT-TRACE-GUIDE-001",
  "eventType": "PAYMENT_COMPLETED",
  "payload": {
    "paymentId": "PAY-TRACE-001",
    "userId": "USER-TRACE",
    "amount": 50.00,
    "currency": "USD"
  }
}
'@

curl.exe -s -i -X POST http://localhost:8080/api/events `
  -H "Content-Type: application/json" `
  -H "X-Correlation-ID: my-custom-trace-12345" `
  -d $customTraceEvent
```

In the HTTP response headers, you will see:
```http
X-Correlation-ID: my-custom-trace-12345
X-Trace-ID: my-custom-trace-12345
```

And in the application logs:
```powershell
docker compose logs app | Select-String "my-custom-trace-12345"
```
Every log line will contain `[traceId=my-custom-trace-12345, eventId=EVT-TRACE-GUIDE-001, eventType=PAYMENT_COMPLETED]`.

### Prometheus Metrics Endpoint

The application exposes real-time Prometheus metrics:

```powershell
curl.exe -s http://localhost:8080/actuator/prometheus | Select-String "events_"
```

Key metrics:
* `events_processed_total{eventType="PAYMENT_COMPLETED",status="SUCCESS"}`
* `events_duplicate_total{eventType="PAYMENT_COMPLETED",source="POSTGRES"}`
* `events_dlq_total{eventType="PAYMENT_COMPLETED",reason="..."}`
* `events_processing_duration_seconds_count`

---

## 22. Troubleshooting Guide

| Problem | Symptoms | What to Check | Solution |
| :--- | :--- | :--- | :--- |
| **Port Already In Use** | `docker compose up` fails with `port is already allocated` (e.g. 5432 or 8080). | Run `netstat -ano \| findstr 5432` or `findstr 8080` to find the conflicting process. | Stop the local PostgreSQL/service, or in `.env` set `HOST_DB_PORT=5433` or `HOST_APP_PORT=8081`. |
| **Docker Engine Not Running** | `failed to connect to docker API: open //./pipe/dockerDesktopLinuxEngine: file not found`. | Docker Desktop is stopped. | Start Docker Desktop from the Windows Start menu and wait for the whale icon in the tray to become steady. |
| **`app` container shows `unhealthy`** | `docker compose ps` displays `(unhealthy)` for `event-processor-app`. | Run `docker compose logs app` to see the stack trace. | Common causes: PostgreSQL or Kafka was slow to become ready. Restart app container: `docker compose restart app`. |
| **PostgreSQL connection refused** | `Connection to localhost:5432 refused`. | Running a host query against `localhost:5432` when postgres is not mapped. | Ensure `HOST_DB_PORT=5432` is set in `.env` or check `docker compose ps`. |
| **Event stuck in `PROCESSING`** | An event remains in `PROCESSING` after an abrupt container kill. | Query `SELECT * FROM processed_events WHERE status = 'PROCESSING';` | By design, after 60 seconds (`IDEMPOTENCY_LEASE_DURATION=60s`), the lease expires and the event will be automatically reclaimed on the next message delivery. |
| **Kafka consumer rebalance messages** | Logs show `(Re-)joining group` or `Discovered group coordinator`. | Normal during Kafka startup. | This is expected standard Kafka partition assignment. When it finishes, you will see `partitions assigned: [events-0, events-1, events-2]`. |
| **`wget` or curl connection refused** | Cannot connect to `http://localhost:8080`. | App container is still compiling or starting. | Run `docker compose logs -f app` and wait for `Started EventProcessorApplication in X seconds`. |

---

## 23. Stopping the System Safely

To stop the containers when you are done testing:

```powershell
docker compose down
```

### What does `docker compose down` do?
* It stops all 4 containers cleanly.
* It removes the containers and the `event-network` bridge network.
* **It preserves your database data!** The named Docker volume `postgres_data` is left intact on your disk. When you start the system again with `docker compose up -d`, all your events, payments, and orders will still be there.

> [!CAUTION]
> **DANGEROUS COMMAND: `docker compose down -v`**
> Notice the `-v` flag (volume). 
> Running `docker compose down -v` **deletes the `postgres_data` volume**, destroying all stored events, payments, orders, and Flyway history. 
> Only use `-v` if you explicitly want to wipe all data and start completely fresh from zero.

---

## 24. Restarting the System

To bring the system back online:

```powershell
docker compose up -d
docker compose ps
```

Wait until all 4 containers display `(healthy)`.
All your previously processed events will still be present in PostgreSQL!

---

## 25. Clean Rebuild (When Code Changes)

If you modify source code or configuration and need to rebuild the application container image from scratch:

```powershell
# 1. Stop the current stack (preserves database data)
docker compose down

# 2. Rebuild the application Docker image without using cached layers
docker compose build --no-cache

# 3. Start the updated stack in background
docker compose up -d

# 4. Verify healthy status
docker compose ps
```

---

## 26. Complete Beginner Workflow Checklist

Follow this quick checklist for your daily testing workflow:

1. [ ] **Start Docker Engine** (Launch Docker Desktop).
2. [ ] **Navigate to workspace:** `cd "E:\Idempotent-Event-Processing-System"`
3. [ ] **Start stack:** `docker compose up -d`
4. [ ] **Wait 30s & check health:** `docker compose ps` (all 4 healthy).
5. [ ] **Verify Actuator:** `curl.exe -s http://localhost:8080/actuator/health` -> `{"status":"UP"}`.
6. [ ] **Submit payment:** Submit `EVT-PAY-001` via `POST /api/events`. Verify `HTTP 202`.
7. [ ] **Inspect DB:** `SELECT * FROM payments;` -> Payment created.
8. [ ] **Inspect Redis:** `docker compose exec redis redis-cli get event-processing:EVT-PAY-001` -> `"COMPLETED"`.
9. [ ] **Test Duplicate:** Resubmit `EVT-PAY-001`. Verify `HTTP 409 Conflict`.
10. [ ] **Submit order:** Submit `EVT-ORD-001` (`ORDER_CREATED`). Verify `HTTP 202` and DB record created.
11. [ ] **Test DLQ:** Submit negative payment (`amount: -50.0`). Verify `status = FAILED` in DB and message in `events.dlq`.
12. [ ] **Stop system safely:** `docker compose down` (do not use `-v`).

---

## 27. Final Verification Checklist

Use this checklist to confirm your manual testing was completely successful:

- [ ] Docker Compose starts all 4 containers (`app`, `postgres`, `kafka`, `redis`).
- [ ] All 4 containers achieve `(healthy)` status.
- [ ] Actuator `/actuator/health` returns `{"status":"UP"}`.
- [ ] `PAYMENT_COMPLETED` event returns `HTTP 202 Accepted`.
- [ ] Payment is persisted in the PostgreSQL `payments` table.
- [ ] `ORDER_CREATED` event returns `HTTP 202 Accepted`.
- [ ] Order is persisted in the PostgreSQL `orders` table.
- [ ] Submitting a duplicate event returns `HTTP 409 Conflict`.
- [ ] No duplicate business row is created in `payments` or `orders`.
- [ ] Invalid event type returns `HTTP 400 Bad Request`.
- [ ] Missing payload fields return `HTTP 400 Bad Request`.
- [ ] Fatal error (e.g. negative amount) marks database status as `FAILED`.
- [ ] Fatal error event is published to `events.dlq`.
- [ ] Redis cache stores `COMPLETED` with a 24-hour TTL.
- [ ] Redis deletion does not break idempotency (PostgreSQL fail-open fallback works).
- [ ] Logs contain `traceId`, `eventId`, and `eventType` in MDC format.
- [ ] Application shuts down cleanly with `docker compose down`.

---

## 28. Interview & Architecture Explanations

### "If someone asks me how to run this project, say..."
> *"The project is fully containerized using Docker Compose. You simply clone the repository and run `docker compose up -d`. This spins up PostgreSQL 16, Apache Kafka 3.8.0 in KRaft mode, Redis 7.2, and our Spring Boot 3 application using a multi-stage Java 21 build. Once all four containers pass their health checks, you can submit events to `http://localhost:8080/api/events`. Database data is persisted in a Docker volume so you can stop and restart the stack safely with `docker compose down`."*

### "How an event moves through the system (Interview Walkthrough)"
> *"1. When a client sends `POST /api/events`, the REST controller validates the payload against our business rules and pre-registers the event in PostgreSQL with status `SUBMITTED`. If the event ID already exists, it is rejected immediately with `HTTP 409 Conflict`.*
>
> *2. If valid and new, the event is published to Kafka topic `events` using the `eventId` as the partition key to preserve ordering, and the client immediately receives `HTTP 202 Accepted`.*
>
> *3. Our asynchronous Kafka consumer picks up the record. First, it checks our Redis fast-path cache. If marked `COMPLETED`, it acknowledges the offset and exits in under 1 millisecond.*
>
> *4. If not in cache, our PostgreSQL-backed Idempotency Engine executes an isolated transaction to acquire an atomic reservation lease with a unique UUID fencing token, transitioning the status to `PROCESSING`.*
>
> *5. The business processor executes and writes the business mutation (e.g. creating a payment) and transitions the status to `COMPLETED` within the same atomic database transaction. If the server crashes during execution, the 60-second lease timeout ensures the event can be safely reclaimed upon redelivery.*
>
> *6. Once committed, the event ID is written to Redis with a 24-hour TTL, and the Kafka consumer commits its offset. If a fatal business validation error occurs, the error handler routes the poison pill to `events.dlq` and updates PostgreSQL status to `FAILED`."*
