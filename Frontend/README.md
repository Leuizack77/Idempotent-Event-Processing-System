# Idempotent Event Processing System — Distributed Observability Frontend

Modern, high-performance distributed systems dashboard built with **Next.js 16**, **React 19**, **Three.js (WebGL)**, and **Tailwind CSS v4**.

---

## ⚡ Fast Preview & Development

### 1. Instant Production Preview (Fastest)
To view the compiled production frontend with maximum performance and zero build wait:
```bash
npm run preview
```
> Starts the optimized server in **~300ms** at [http://localhost:3000](http://localhost:3000).

### 2. Development Mode
To start with Turbopack Hot Module Replacement (HMR):
```bash
npm run dev
```

### 3. Production Build
```bash
npm run build
```

---

## 🌟 Key Features

1. **3D WebGL Architecture Topology (`Cluster3DCanvas`)**:
   - Interactive 3D scene rendered with Three.js.
   - Live pulsating nodes: API Gateway, Kafka Cluster (with partition rings), Redis L1 Cache (with pulse halo), PostgreSQL Primary (with fencing token ring), and Dead Letter Queue (DLQ).
   - Animated Bézier curves with photon particle packet streams reflecting active traffic.
   - Orbit controls, hover raycasting, interactive node inspection tooltips, and manual burst controls.
   - Fully touch-responsive on mobile devices (dynamic camera distance & gesture isolation).

2. **3D Extruded Metrics Bar Chart (`Metrics3DChart`)**:
   - WebGL 3D extruded metallic pillars showing Ingested, L1 Deduplicated, Committed, and Dead-Lettered event counts in real-time.

3. **Chaos & Concurrency Studio (`ChaosStudio`)**:
   - **Double-Tap Test**: Fires duplicate event IDs within 50ms to demonstrate L1/L2 deduplication.
   - **Concurrency Storm**: Fires 3–25 concurrent duplicate requests simultaneously to prove isolation under extreme lock contention.
   - **Poison Pill / DLQ Test**: Simulates payload corruption and validates routing to Dead Letter Queue.
   - **Redis Fail-Open Switch**: Toggles Redis cache outage simulation to demonstrate seamless PostgreSQL transactional fallback.

4. **Split-Screen Exact Proof (`SplitScreenProof`)**:
   - Visually contrasts Kafka At-Least-Once delivery stream with PostgreSQL Exactly-Once committed state.

5. **Distributed Trace Waterfall (`TraceWaterfall`)**:
   - Gantt-style execution breakdown displaying MDC Trace IDs, span latencies, and execution phases.

6. **Event Ledger & Business Records**:
   - Real-time event log with deduplication verdicts, payload inspection, and business table state (Payment records).

---

## 📱 Responsive Design Support
- Built with Tailwind CSS v4 fluid breakpoints (`sm`, `md`, `lg`, `xl`).
- Dynamic WebGL camera perspective calculation for portrait and landscape mobile screens.
- Touch scroll-safe container with non-interfering orbital canvas controls.
