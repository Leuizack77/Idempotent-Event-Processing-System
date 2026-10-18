export type EventStatus = 'SUBMITTED' | 'RECEIVED' | 'PROCESSING' | 'COMPLETED' | 'FAILED';

export type DeduplicationSource = 'NONE' | 'REDIS_FAST_PATH' | 'POSTGRES_CONSTRAINT' | 'FENCING_TOKEN_LOCK';

export interface TraceSpan {
  id: string;
  name: string;
  service: 'HTTP_CONTROLLER' | 'KAFKA_BROKER' | 'REDIS_CACHE' | 'POSTGRES_DB' | 'BUSINESS_EXECUTOR';
  durationMs: number;
  offsetMs: number;
  status: 'OK' | 'HIT' | 'MISS' | 'LOCKED' | 'ERROR';
  details: string;
}

export interface TraceEvent {
  traceId: string;
  eventId: string;
  spans: TraceSpan[];
  totalDurationMs: number;
}

export interface EventRequestDto {
  eventId: string;
  eventType: 'PAYMENT_PROCESSED' | 'ORDER_CREATED' | string;
  payload: Record<string, unknown>;
}

export interface EventResponseDto {
  eventId: string;
  status: EventStatus;
  message: string;
  submittedAt: string;
}

export interface ProcessedEventRecord {
  id: string;
  eventId: string;
  eventType: string;
  status: EventStatus;
  processingToken: string;
  retryCount: number;
  deduplicationSource: DeduplicationSource;
  durationMs: number;
  errorMessage?: string | null;
  payload: Record<string, unknown>;
  traceId: string;
  spans?: TraceSpan[];
  createdAt: string;
  updatedAt: string;
}

export interface PaymentRecord {
  id: number;
  paymentId: string;
  userId: string;
  amount: number;
  currency: string;
  createdAt: string;
}

export interface OrderRecord {
  id: number;
  orderId: string;
  customerId: string;
  totalAmount: number;
  itemCount: number;
  createdAt: string;
}

export interface ClusterStatus {
  app: 'UP' | 'DOWN' | 'STARTING';
  postgres: 'UP' | 'DOWN';
  kafka: 'UP' | 'DOWN';
  redis: 'UP' | 'DOWN';
}

export interface SystemMetrics {
  totalSubmitted: number;
  totalProcessed: number;
  totalDuplicatesBlocked: number;
  redisCacheHits: number;
  postgresConstraintHits: number;
  dlqTotal: number;
  averageLatencyMs: number;
  redisLatencyMs: number;
  postgresLatencyMs: number;
}
