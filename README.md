# Idempotent Event Processing System

A production-grade, fault-tolerant distributed event processing system ensuring **Exactly-Once Semantics** over **At-Least-Once Kafka streams** using two-phase idempotency (Redis L1 cache + PostgreSQL transactional fencing).

---

## 📂 Repository Structure

```
Idempotent-Event-Processing-System/
├── Backend/                 # Java 21 & Spring Boot 3.3.4 Application
│   ├── src/                 # Enterprise application & integration tests
│   ├── pom.xml              # Maven dependencies & build specification
│   ├── Dockerfile           # Multi-stage production container build
│   ├── docker-compose.yml   # Kafka, Zookeeper, Redis & PostgreSQL infra
│   └── docs/                # Architecture diagrams, benchmark reports & ADRs
│
├── Frontend/                # Next.js 16 + React 19 + Three.js Observability UI
│   ├── app/                 # Next.js App Router (page, layout, styles)
│   ├── components/          # 3D WebGL Canvas, Chaos Studio, Metrics & Proof views
│   ├── lib/                 # Distributed tracing, mock sync & backend client
│   └── package.json         # Frontend dependencies & preview scripts
│
└── package.json             # Root workspace runner for convenience
```

---

## 🚀 Quick Start

### 1. Fast Frontend Preview
You can run `npm run preview` from the project root or from inside `Frontend/`:
```bash
npm run preview
```
> Instantly launches the optimized production frontend at [http://localhost:3000](http://localhost:3000) in **~300ms**.

To run in development mode with HMR:
```bash
npm run dev
```

### 2. Backend Services
Navigate to `Backend/` to run infrastructure and Spring Boot services:
```bash
cd Backend

# Start Kafka, Redis & PostgreSQL containers
docker-compose up -d

# Build and run backend application
mvn spring-boot:run
```
Backend API will be accessible on `http://localhost:8080`.
Frontend automatically proxies `/api/backend/*` to `http://localhost:8080/api/*`.
