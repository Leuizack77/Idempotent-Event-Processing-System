import { 
  ClusterStatus, 
  DeduplicationSource, 
  EventRequestDto, 
  EventResponseDto, 
  OrderRecord, 
  PaymentRecord, 
  ProcessedEventRecord, 
  SystemMetrics,
  TraceSpan
} from './types';

function createSpansForFirstRun(eventId: string, token: string): TraceSpan[] {
  return [
    {
      id: 'span-1',
      name: 'POST /api/events (Ingest)',
      service: 'HTTP_CONTROLLER',
      durationMs: 4,
      offsetMs: 0,
      status: 'OK',
      details: 'Payload validated. Inserting initial SUBMITTED state into PostgreSQL.',
    },
    {
      id: 'span-2',
      name: 'INSERT INTO processed_events (SUBMITTED)',
      service: 'POSTGRES_DB',
      durationMs: 6,
      offsetMs: 4,
      status: 'OK',
      details: 'Durable submission record stored before transport publish.',
    },
    {
      id: 'span-3',
      name: `Kafka Producer: send(topic="events", key="${eventId}")`,
      service: 'KAFKA_BROKER',
      durationMs: 8,
      offsetMs: 10,
      status: 'OK',
      details: 'Produced with key partition affinity & MDC trace headers attached.',
    },
    {
      id: 'span-4',
      name: 'Kafka Consumer: poll() (Group: event-processor-group)',
      service: 'KAFKA_BROKER',
      durationMs: 5,
      offsetMs: 18,
      status: 'OK',
      details: 'Consumed record. Thread assigned from event listener container.',
    },
    {
      id: 'span-5',
      name: `Redis: GET event-processing:${eventId}`,
      service: 'REDIS_CACHE',
      durationMs: 1,
      offsetMs: 23,
      status: 'MISS',
      details: 'Key not found in L1 cache. Falling through to PostgreSQL authoritative check.',
    },
    {
      id: 'span-6',
      name: 'PostgreSQL: Acquire Lease & Token (REQUIRES_NEW)',
      service: 'POSTGRES_DB',
      durationMs: 7,
      offsetMs: 24,
      status: 'LOCKED',
      details: `State transitioned to PROCESSING. Acquired UUID Token: ${token.substring(0, 8)}...`,
    },
    {
      id: 'span-7',
      name: 'Business Execution: Insert Domain Entity',
      service: 'BUSINESS_EXECUTOR',
      durationMs: 11,
      offsetMs: 31,
      status: 'OK',
      details: 'ACID transaction: Created business entity row and asserted invariants.',
    },
    {
      id: 'span-8',
      name: 'PostgreSQL: UPDATE to COMPLETED WHERE token = :token',
      service: 'POSTGRES_DB',
      durationMs: 5,
      offsetMs: 42,
      status: 'OK',
      details: 'Token matched! Exactly-once state mutation committed to PostgreSQL.',
    },
    {
      id: 'span-9',
      name: `Redis: SET event-processing:${eventId} (TTL: 24h)`,
      service: 'REDIS_CACHE',
      durationMs: 1,
      offsetMs: 47,
      status: 'OK',
      details: 'Populated fast-path cache. Future duplicate deliveries will short-circuit.',
    },
  ];
}

function createSpansForRedisFastPath(eventId: string, token: string): TraceSpan[] {
  return [
    {
      id: 'span-1',
      name: 'POST /api/events (Ingest Duplicate)',
      service: 'HTTP_CONTROLLER',
      durationMs: 2,
      offsetMs: 0,
      status: 'OK',
      details: 'Incoming duplicate HTTP request received.',
    },
    {
      id: 'span-2',
      name: `Redis: GET event-processing:${eventId}`,
      service: 'REDIS_CACHE',
      durationMs: 1,
      offsetMs: 2,
      status: 'HIT',
      details: 'FAST-PATH HIT: Status is COMPLETED. Intercepting before database!',
    },
    {
      id: 'span-3',
      name: 'Fast-Path Short-Circuit (Skip Kafka & DB)',
      service: 'HTTP_CONTROLLER',
      durationMs: 1,
      offsetMs: 3,
      status: 'OK',
      details: `Returned HTTP 202 with cached COMPLETED state. Zero database rows touched. Latency: 4ms.`,
    },
  ];
}

function createSpansForPostgresConstraint(eventId: string, token: string): TraceSpan[] {
  return [
    {
      id: 'span-1',
      name: 'POST /api/events (Concurrent Collision)',
      service: 'HTTP_CONTROLLER',
      durationMs: 3,
      offsetMs: 0,
      status: 'OK',
      details: 'Concurrent request arrived during active lease processing.',
    },
    {
      id: 'span-2',
      name: `Redis: GET event-processing:${eventId}`,
      service: 'REDIS_CACHE',
      durationMs: 1,
      offsetMs: 3,
      status: 'MISS',
      details: 'Event currently in flight in DB, not yet completed in Redis.',
    },
    {
      id: 'span-3',
      name: 'PostgreSQL: INSERT / Transition (Constraint Collision)',
      service: 'POSTGRES_DB',
      durationMs: 12,
      offsetMs: 4,
      status: 'LOCKED',
      details: 'uq_processed_events_event_id or active lease asserted. Rolled back safely.',
    },
    {
      id: 'span-4',
      name: 'Fencing Check: 0 rows updated',
      service: 'POSTGRES_DB',
      durationMs: 3,
      offsetMs: 16,
      status: 'OK',
      details: 'Concurrent worker failed token assertion. Safely acknowledged with zero double-mutation.',
    },
  ];
}

const INITIAL_EVENTS: ProcessedEventRecord[] = [
  {
    id: 'evt-001',
    eventId: 'PAY-TXN-8812',
    eventType: 'PAYMENT_PROCESSED',
    status: 'COMPLETED',
    processingToken: '4b3d7499-4c12-4f81-80bb-6925927ad001',
    retryCount: 0,
    deduplicationSource: 'NONE',
    durationMs: 48,
    errorMessage: null,
    payload: { paymentId: 'PAY-TXN-8812', userId: 'user_al1ce', amount: 149.99, currency: 'USD' },
    traceId: 'tr-9901-aa4',
    spans: createSpansForFirstRun('PAY-TXN-8812', '4b3d7499-4c12-4f81-80bb-6925927ad001'),
    createdAt: new Date(Date.now() - 1000 * 60 * 15).toISOString(),
    updatedAt: new Date(Date.now() - 1000 * 60 * 15).toISOString(),
  },
  {
    id: 'evt-002',
    eventId: 'PAY-TXN-8812',
    eventType: 'PAYMENT_PROCESSED',
    status: 'COMPLETED',
    processingToken: '4b3d7499-4c12-4f81-80bb-6925927ad001',
    retryCount: 0,
    deduplicationSource: 'REDIS_FAST_PATH',
    durationMs: 4,
    errorMessage: null,
    payload: { paymentId: 'PAY-TXN-8812', userId: 'user_al1ce', amount: 149.99, currency: 'USD' },
    traceId: 'tr-9902-bb7',
    spans: createSpansForRedisFastPath('PAY-TXN-8812', '4b3d7499-4c12-4f81-80bb-6925927ad001'),
    createdAt: new Date(Date.now() - 1000 * 60 * 14).toISOString(),
    updatedAt: new Date(Date.now() - 1000 * 60 * 14).toISOString(),
  },
  {
    id: 'evt-003',
    eventId: 'ORD-CART-4991',
    eventType: 'ORDER_CREATED',
    status: 'COMPLETED',
    processingToken: '8f12cc7a-3908-41ae-9671-55db0c1289fe',
    retryCount: 0,
    deduplicationSource: 'NONE',
    durationMs: 54,
    errorMessage: null,
    payload: { orderId: 'ORD-CART-4991', customerId: 'cust_bob77', totalAmount: 499.0, itemCount: 3 },
    traceId: 'tr-8833-dd1',
    spans: createSpansForFirstRun('ORD-CART-4991', '8f12cc7a-3908-41ae-9671-55db0c1289fe'),
    createdAt: new Date(Date.now() - 1000 * 60 * 8).toISOString(),
    updatedAt: new Date(Date.now() - 1000 * 60 * 8).toISOString(),
  },
  {
    id: 'evt-004',
    eventId: 'ORD-CART-4991',
    eventType: 'ORDER_CREATED',
    status: 'COMPLETED',
    processingToken: '8f12cc7a-3908-41ae-9671-55db0c1289fe',
    retryCount: 0,
    deduplicationSource: 'POSTGRES_CONSTRAINT',
    durationMs: 19,
    errorMessage: null,
    payload: { orderId: 'ORD-CART-4991', customerId: 'cust_bob77', totalAmount: 499.0, itemCount: 3 },
    traceId: 'tr-8834-dd2',
    spans: createSpansForPostgresConstraint('ORD-CART-4991', '8f12cc7a-3908-41ae-9671-55db0c1289fe'),
    createdAt: new Date(Date.now() - 1000 * 60 * 7).toISOString(),
    updatedAt: new Date(Date.now() - 1000 * 60 * 7).toISOString(),
  },
  {
    id: 'evt-005',
    eventId: 'BAD-PAYLOAD-999',
    eventType: 'PAYMENT_PROCESSED',
    status: 'FAILED',
    processingToken: '992a013d-1873-4217-bc45-e64e9a3b90aa',
    retryCount: 3,
    deduplicationSource: 'NONE',
    durationMs: 3120,
    errorMessage: 'Terminal Kafka retry exhausted: Invalid currency [ERR_MALFORMED_SYMBOL]. Routed to events.dlq',
    payload: { paymentId: 'BAD-PAYLOAD-999', userId: 'user_malformed', amount: -50, currency: 'INVALID_XXX' },
    traceId: 'tr-9999-err',
    createdAt: new Date(Date.now() - 1000 * 60 * 2).toISOString(),
    updatedAt: new Date(Date.now() - 1000 * 60 * 2).toISOString(),
  },
];

const INITIAL_PAYMENTS: PaymentRecord[] = [
  {
    id: 1,
    paymentId: 'PAY-TXN-8812',
    userId: 'user_al1ce',
    amount: 149.99,
    currency: 'USD',
    createdAt: new Date(Date.now() - 1000 * 60 * 15).toISOString(),
  },
];

const INITIAL_ORDERS: OrderRecord[] = [
  {
    id: 1,
    orderId: 'ORD-CART-4991',
    customerId: 'cust_bob77',
    totalAmount: 499.0,
    itemCount: 3,
    createdAt: new Date(Date.now() - 1000 * 60 * 8).toISOString(),
  },
];

class EventProcessingService {
  private events: ProcessedEventRecord[] = [...INITIAL_EVENTS];
  private payments: PaymentRecord[] = [...INITIAL_PAYMENTS];
  private orders: OrderRecord[] = [...INITIAL_ORDERS];
  private redisSimulatedDown: boolean = false;

  isRedisDown(): boolean {
    return this.redisSimulatedDown;
  }

  toggleRedisSimulatedDown(): boolean {
    this.redisSimulatedDown = !this.redisSimulatedDown;
    return this.redisSimulatedDown;
  }

  async checkClusterHealth(): Promise<ClusterStatus> {
    try {
      const res = await fetch('/actuator/health', { method: 'GET', signal: AbortSignal.timeout(2000) });
      if (res.ok) {
        return {
          app: 'UP',
          postgres: 'UP',
          kafka: 'UP',
          redis: this.redisSimulatedDown ? 'DOWN' : 'UP',
        };
      }
    } catch {
      // Backend not running locally
    }

    return {
      app: 'DOWN',
      postgres: 'DOWN',
      kafka: 'DOWN',
      redis: this.redisSimulatedDown ? 'DOWN' : 'DOWN',
    };
  }

  getMetrics(): SystemMetrics {
    const totalSubmitted = this.events.length;
    const totalProcessed = this.events.filter(e => e.status === 'COMPLETED').length;
    const redisHits = this.events.filter(e => e.deduplicationSource === 'REDIS_FAST_PATH').length;
    const pgHits = this.events.filter(e => e.deduplicationSource === 'POSTGRES_CONSTRAINT').length;
    const dlqCount = this.events.filter(e => e.status === 'FAILED').length;
    const totalDuplicates = redisHits + pgHits;
    
    const validDurations = this.events.filter(e => e.durationMs > 0);
    const avgLatency = validDurations.length > 0 
      ? Math.round(validDurations.reduce((acc, curr) => acc + curr.durationMs, 0) / validDurations.length)
      : 0;

    return {
      totalSubmitted,
      totalProcessed,
      totalDuplicatesBlocked: totalDuplicates,
      redisCacheHits: redisHits,
      postgresConstraintHits: pgHits,
      dlqTotal: dlqCount,
      averageLatencyMs: avgLatency,
      redisLatencyMs: 3,
      postgresLatencyMs: 42,
    };
  }

  getEvents(): ProcessedEventRecord[] {
    return [...this.events].reverse();
  }

  getPayments(): PaymentRecord[] {
    return [...this.payments].reverse();
  }

  getOrders(): OrderRecord[] {
    return [...this.orders].reverse();
  }

  async submitEvent(request: EventRequestDto): Promise<{ response: EventResponseDto; source: DeduplicationSource }> {
    const existing = this.events.find(e => e.eventId === request.eventId && e.status === 'COMPLETED');
    const now = new Date().toISOString();
    let deduplicationSource: DeduplicationSource = 'NONE';
    const token = existing?.processingToken || crypto.randomUUID();
    const traceId = `tr-${Date.now().toString(36)}-${Math.floor(Math.random() * 1000)}`;

    let spans: TraceSpan[];
    let durationMs = 0;

    if (existing) {
      if (!this.redisSimulatedDown) {
        // Normal state: Redis Fast-Path hit
        deduplicationSource = 'REDIS_FAST_PATH';
        durationMs = Math.floor(Math.random() * 3) + 2; // 2-4ms
        spans = createSpansForRedisFastPath(request.eventId, token);
      } else {
        // Redis is simulated DOWN: Fail-open to PostgreSQL constraint
        deduplicationSource = 'POSTGRES_CONSTRAINT';
        durationMs = Math.floor(Math.random() * 15) + 25; // 25-40ms
        spans = createSpansForPostgresConstraint(request.eventId, token);
      }
    } else {
      // First run: Full atomic transaction
      deduplicationSource = 'NONE';
      durationMs = Math.floor(Math.random() * 20) + 38; // 38-58ms
      spans = createSpansForFirstRun(request.eventId, token);

      if (request.eventType === 'PAYMENT_PROCESSED') {
        const payload = request.payload as unknown as Record<string, unknown>;
        const amount = typeof payload.amount === 'number' ? payload.amount : 100.0;
        const currency = typeof payload.currency === 'string' ? payload.currency : 'USD';
        const userId = typeof payload.userId === 'string' ? payload.userId : 'user_default';
        const paymentId = typeof payload.paymentId === 'string' ? payload.paymentId : request.eventId;

        this.payments.push({
          id: this.payments.length + 1,
          paymentId,
          userId,
          amount,
          currency,
          createdAt: now,
        });
      } else if (request.eventType === 'ORDER_CREATED') {
        const payload = request.payload as unknown as Record<string, unknown>;
        const totalAmount = typeof payload.totalAmount === 'number' ? payload.totalAmount : 250.0;
        const itemCount = typeof payload.itemCount === 'number' ? payload.itemCount : 2;
        const customerId = typeof payload.customerId === 'string' ? payload.customerId : 'cust_default';
        const orderId = typeof payload.orderId === 'string' ? payload.orderId : request.eventId;

        this.orders.push({
          id: this.orders.length + 1,
          orderId,
          customerId,
          totalAmount,
          itemCount,
          createdAt: now,
        });
      }
    }

    const newRecord: ProcessedEventRecord = {
      id: `evt-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
      eventId: request.eventId,
      eventType: request.eventType,
      status: 'COMPLETED',
      processingToken: token,
      retryCount: 0,
      deduplicationSource,
      durationMs,
      errorMessage: null,
      payload: request.payload,
      traceId,
      spans,
      createdAt: now,
      updatedAt: now,
    };

    this.events.push(newRecord);

    const response: EventResponseDto = {
      eventId: request.eventId,
      status: 'SUBMITTED',
      message: deduplicationSource === 'NONE'
        ? 'Event accepted and queued for idempotent processing'
        : `Duplicate intercepted safely via ${deduplicationSource}. Zero double mutations!`,
      submittedAt: now,
    };

    return { response, source: deduplicationSource };
  }

  async triggerPoisonPill(eventId: string): Promise<ProcessedEventRecord> {
    const now = new Date().toISOString();
    const token = crypto.randomUUID();

    const record: ProcessedEventRecord = {
      id: `evt-err-${Date.now()}`,
      eventId,
      eventType: 'PAYMENT_PROCESSED',
      status: 'FAILED',
      processingToken: token,
      retryCount: 3,
      deduplicationSource: 'NONE',
      durationMs: 3150,
      errorMessage: 'Terminal retry failure: Schema violation [ERR_POISON_PILL]. Isolated to events.dlq.',
      payload: { eventId, error: 'MALFORMED_PAYLOAD_TRIGGERED', fatal: true },
      traceId: `tr-dlq-${Math.floor(1000 + Math.random() * 9000)}`,
      createdAt: now,
      updatedAt: now,
    };

    this.events.push(record);
    return record;
  }

  resetAll(): void {
    this.events = [...INITIAL_EVENTS];
    this.payments = [...INITIAL_PAYMENTS];
    this.orders = [...INITIAL_ORDERS];
  }
}

export const eventService = new EventProcessingService();
