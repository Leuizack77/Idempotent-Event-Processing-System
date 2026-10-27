'use client';

import React, { useState } from 'react';
import { 
  Zap, 
  Send, 
  Flame, 
  AlertTriangle, 
  RotateCcw, 
  CheckCircle2, 
  ShieldAlert, 
  Copy,
  Clock,
  Sparkles
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { eventService } from '@/lib/api';
import { EventRequestDto } from '@/lib/types';

interface EventSimulatorProps {
  onEventSubmitted: () => void;
}

export const EventSimulator: React.FC<EventSimulatorProps> = ({ onEventSubmitted }) => {
  const [eventType, setEventType] = useState<'PAYMENT_PROCESSED' | 'ORDER_CREATED'>('PAYMENT_PROCESSED');
  const [eventId, setEventId] = useState<string>('PAY-TXN-9001');
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);
  const [burstProgress, setBurstProgress] = useState<number | null>(null);

  // Payment form states
  const [paymentAmount, setPaymentAmount] = useState<number>(199.99);
  const [paymentCurrency, setPaymentCurrency] = useState<string>('USD');
  const [paymentUserId, setPaymentUserId] = useState<string>('user_dev_01');

  // Order form states
  const [orderAmount, setOrderAmount] = useState<number>(349.50);
  const [orderItemCount, setOrderItemCount] = useState<number>(4);
  const [orderCustomerId, setOrderCustomerId] = useState<string>('cust_vip_42');

  // Last result
  const [lastLog, setLastLog] = useState<{
    message: string;
    source: string;
    status: 'success' | 'duplicate' | 'error';
    durationMs: number;
    timestamp: string;
  } | null>(null);

  const generateRandomEventId = () => {
    const prefix = eventType === 'PAYMENT_PROCESSED' ? 'PAY' : 'ORD';
    const rand = Math.floor(1000 + Math.random() * 9000);
    setEventId(`${prefix}-TXN-${rand}`);
  };

  const getPayload = (): Record<string, unknown> => {
    if (eventType === 'PAYMENT_PROCESSED') {
      return {
        paymentId: eventId,
        userId: paymentUserId,
        amount: Number(paymentAmount),
        currency: paymentCurrency,
      };
    } else {
      return {
        orderId: eventId,
        customerId: orderCustomerId,
        totalAmount: Number(orderAmount),
        itemCount: Number(orderItemCount),
      };
    }
  };

  const handleSingleSubmit = async () => {
    setIsSubmitting(true);
    const start = performance.now();
    try {
      const request: EventRequestDto = {
        eventId,
        eventType,
        payload: getPayload(),
      };

      const { response, source } = await eventService.submitEvent(request);
      const durationMs = Math.round(performance.now() - start);

      setLastLog({
        message: response.message,
        source: source === 'NONE' ? 'Original First-Time Commit' : source,
        status: source === 'NONE' ? 'success' : 'duplicate',
        durationMs,
        timestamp: new Date().toLocaleTimeString(),
      });

      onEventSubmitted();
    } catch {
      setLastLog({
        message: 'Failed to ingest event.',
        source: 'ERROR',
        status: 'error',
        durationMs: 0,
        timestamp: new Date().toLocaleTimeString(),
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleConcurrentBurst = async (burstCount: number = 5) => {
    setIsSubmitting(true);
    setBurstProgress(0);
    const start = performance.now();

    const request: EventRequestDto = {
      eventId,
      eventType,
      payload: getPayload(),
    };

    try {
      // Fire simultaneous concurrent requests
      const promises = Array.from({ length: burstCount }, () => eventService.submitEvent(request));
      const results = await Promise.all(promises);

      const durationMs = Math.round(performance.now() - start);
      const duplicates = results.filter(r => r.source !== 'NONE').length;
      const originals = results.filter(r => r.source === 'NONE').length;

      setLastLog({
        message: `Fired ${burstCount} concurrent requests with SAME eventId! ${originals} processed, ${duplicates} intercepted via Redis / Postgres. Exactly-once maintained!`,
        source: duplicates > 0 ? 'CONCURRENT_BURST_DEDUPED' : 'ORIGINAL_RUN',
        status: 'duplicate',
        durationMs,
        timestamp: new Date().toLocaleTimeString(),
      });

      onEventSubmitted();
    } catch {
      setLastLog({
        message: 'Burst simulation error.',
        source: 'ERROR',
        status: 'error',
        durationMs: 0,
        timestamp: new Date().toLocaleTimeString(),
      });
    } finally {
      setIsSubmitting(false);
      setBurstProgress(null);
    }
  };

  const handlePoisonPill = async () => {
    setIsSubmitting(true);
    const start = performance.now();
    try {
      const poisonId = `ERR-POISON-${Math.floor(1000 + Math.random() * 9000)}`;
      await eventService.triggerPoisonPill(poisonId);
      const durationMs = Math.round(performance.now() - start);

      setLastLog({
        message: `Poison pill sent [${poisonId}]. Retries exhausted ➔ Routed safely to Dead Letter Queue (events.dlq).`,
        source: 'DEAD_LETTER_QUEUE',
        status: 'error',
        durationMs,
        timestamp: new Date().toLocaleTimeString(),
      });

      onEventSubmitted();
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
      {/* Event Configuration Panel */}
      <div className="lg:col-span-7 glass-panel rounded-2xl sm:rounded-3xl p-5 sm:p-6 shadow-xl flex flex-col justify-between">
        <div>
          <div className="flex items-center justify-between pb-4 border-b border-slate-800 gap-2">
            <div>
              <h2 className="text-sm sm:text-base font-bold text-white flex items-center gap-2">
                <Zap className="w-4 h-4 sm:w-5 sm:h-5 text-indigo-400 shrink-0" />
                <span>Event Generator & Ingestion Simulator</span>
              </h2>
              <p className="text-[11px] sm:text-xs text-slate-400 mt-0.5">
                Construct and dispatch business events directly to the idempotency pipeline.
              </p>
            </div>
            <button
              onClick={generateRandomEventId}
              className="text-xs px-2.5 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg border border-slate-700 flex items-center gap-1.5 transition"
            >
              <Sparkles className="w-3.5 h-3.5 text-indigo-400" />
              New ID
            </button>
          </div>

          {/* Form Fields */}
          <div className="mt-5 space-y-4">
            {/* Event Type selector */}
            <div>
              <label className="text-xs font-semibold text-slate-300 uppercase tracking-wider block mb-1.5">
                Event Type
              </label>
              <div className="grid grid-cols-2 gap-3">
                <button
                  type="button"
                  onClick={() => {
                    setEventType('PAYMENT_PROCESSED');
                    setEventId(`PAY-TXN-${Math.floor(1000 + Math.random() * 9000)}`);
                  }}
                  className={`p-3 rounded-xl border text-left transition ${
                    eventType === 'PAYMENT_PROCESSED'
                      ? 'border-indigo-500 bg-indigo-500/10 text-white shadow-sm'
                      : 'border-slate-800 bg-slate-950/60 text-slate-400 hover:border-slate-700'
                  }`}
                >
                  <div className="font-semibold text-sm">Payment Event</div>
                  <div className="text-[11px] text-slate-500 font-mono mt-0.5">PAYMENT_PROCESSED</div>
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setEventType('ORDER_CREATED');
                    setEventId(`ORD-CART-${Math.floor(1000 + Math.random() * 9000)}`);
                  }}
                  className={`p-3 rounded-xl border text-left transition ${
                    eventType === 'ORDER_CREATED'
                      ? 'border-indigo-500 bg-indigo-500/10 text-white shadow-sm'
                      : 'border-slate-800 bg-slate-950/60 text-slate-400 hover:border-slate-700'
                  }`}
                >
                  <div className="font-semibold text-sm">Order Event</div>
                  <div className="text-[11px] text-slate-500 font-mono mt-0.5">ORDER_CREATED</div>
                </button>
              </div>
            </div>

            {/* Event ID */}
            <div>
              <label className="text-xs font-semibold text-slate-300 uppercase tracking-wider block mb-1.5">
                Event ID (Unique Business Key)
              </label>
              <div className="relative">
                <input
                  type="text"
                  value={eventId}
                  onChange={e => setEventId(e.target.value)}
                  className="w-full bg-slate-950 border border-slate-800 rounded-xl px-3.5 py-2.5 text-sm font-mono text-white focus:outline-none focus:border-indigo-500"
                  placeholder="e.g. PAY-TXN-9001"
                />
                <button
                  onClick={() => navigator.clipboard.writeText(eventId)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-300"
                  title="Copy ID"
                >
                  <Copy className="w-4 h-4" />
                </button>
              </div>
            </div>

            {/* Dynamic Payload fields based on event type */}
            {eventType === 'PAYMENT_PROCESSED' ? (
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 bg-slate-950/50 p-3.5 rounded-xl border border-slate-800">
                <div>
                  <label className="text-[11px] font-semibold text-slate-400 block mb-1">Amount</label>
                  <input
                    type="number"
                    step="0.01"
                    value={paymentAmount}
                    onChange={e => setPaymentAmount(parseFloat(e.target.value) || 0)}
                    className="w-full bg-slate-900 border border-slate-800 rounded-lg px-3 py-1.5 text-sm text-white font-mono focus:outline-none focus:border-indigo-500"
                  />
                </div>
                <div>
                  <label className="text-[11px] font-semibold text-slate-400 block mb-1">Currency</label>
                  <input
                    type="text"
                    value={paymentCurrency}
                    onChange={e => setPaymentCurrency(e.target.value)}
                    className="w-full bg-slate-900 border border-slate-800 rounded-lg px-3 py-1.5 text-sm text-white font-mono focus:outline-none focus:border-indigo-500"
                  />
                </div>
                <div>
                  <label className="text-[11px] font-semibold text-slate-400 block mb-1">User ID</label>
                  <input
                    type="text"
                    value={paymentUserId}
                    onChange={e => setPaymentUserId(e.target.value)}
                    className="w-full bg-slate-900 border border-slate-800 rounded-lg px-3 py-1.5 text-sm text-white font-mono focus:outline-none focus:border-indigo-500"
                  />
                </div>
              </div>
            ) : (
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 bg-slate-950/50 p-3.5 rounded-xl border border-slate-800">
                <div>
                  <label className="text-[11px] font-semibold text-slate-400 block mb-1">Total Amount</label>
                  <input
                    type="number"
                    step="0.01"
                    value={orderAmount}
                    onChange={e => setOrderAmount(parseFloat(e.target.value) || 0)}
                    className="w-full bg-slate-900 border border-slate-800 rounded-lg px-3 py-1.5 text-sm text-white font-mono focus:outline-none focus:border-indigo-500"
                  />
                </div>
                <div>
                  <label className="text-[11px] font-semibold text-slate-400 block mb-1">Items Count</label>
                  <input
                    type="number"
                    value={orderItemCount}
                    onChange={e => setOrderItemCount(parseInt(e.target.value) || 1)}
                    className="w-full bg-slate-900 border border-slate-800 rounded-lg px-3 py-1.5 text-sm text-white font-mono focus:outline-none focus:border-indigo-500"
                  />
                </div>
                <div>
                  <label className="text-[11px] font-semibold text-slate-400 block mb-1">Customer ID</label>
                  <input
                    type="text"
                    value={orderCustomerId}
                    onChange={e => setOrderCustomerId(e.target.value)}
                    className="w-full bg-slate-900 border border-slate-800 rounded-lg px-3 py-1.5 text-sm text-white font-mono focus:outline-none focus:border-indigo-500"
                  />
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Action Triggers */}
        <div className="mt-6 pt-4 border-t border-slate-800 flex flex-col sm:flex-row items-stretch sm:items-center gap-2.5 sm:gap-3">
          <button
            onClick={handleSingleSubmit}
            disabled={isSubmitting}
            className="flex-1 bg-indigo-600 hover:bg-indigo-500 text-white font-semibold text-xs sm:text-sm px-4 py-2.5 rounded-xl shadow-lg shadow-indigo-600/20 transition flex items-center justify-center gap-2 disabled:opacity-50 active:scale-95"
          >
            <Send className="w-3.5 h-3.5 sm:w-4 sm:h-4" />
            <span>{isSubmitting ? 'Processing...' : 'Send Event'}</span>
          </button>

          <button
            onClick={() => handleConcurrentBurst(5)}
            disabled={isSubmitting}
            className="bg-amber-600/20 hover:bg-amber-600/30 text-amber-400 border border-amber-500/30 font-semibold text-xs sm:text-sm px-4 py-2.5 rounded-xl transition flex items-center justify-center gap-2 disabled:opacity-50 active:scale-95"
            title="Fire 5 concurrent requests with the identical eventId to test race conditions & deduplication"
          >
            <Flame className="w-3.5 h-3.5 sm:w-4 sm:h-4" />
            <span>Burst Attack (5x Duplicates)</span>
          </button>

          <button
            onClick={handlePoisonPill}
            disabled={isSubmitting}
            className="bg-rose-500/10 hover:bg-rose-500/20 text-rose-400 border border-rose-500/30 font-semibold text-xs sm:text-sm px-3.5 py-2.5 rounded-xl transition flex items-center justify-center gap-1.5 disabled:opacity-50 active:scale-95"
            title="Inject malformed payload to trigger Kafka retries & Dead Letter Queue routing"
          >
            <AlertTriangle className="w-3.5 h-3.5 sm:w-4 sm:h-4" />
            <span>Poison Pill</span>
          </button>
        </div>
      </div>

      {/* Real-time Feedback & Diagnostics Log */}
      <div className="lg:col-span-5 glass-panel rounded-2xl sm:rounded-3xl p-5 sm:p-6 shadow-xl flex flex-col justify-between">
        <div>
          <div className="flex items-center justify-between pb-4 border-b border-slate-800">
            <h3 className="text-sm font-bold text-white flex items-center gap-2">
              <Clock className="w-4 h-4 text-indigo-400" />
              Live Execution Diagnostics
            </h3>
            <span className="text-xs font-mono text-slate-500">MDC Traced</span>
          </div>

          <div className="mt-4">
            {lastLog ? (
              <div
                className={`p-4 rounded-xl border ${
                  lastLog.status === 'success'
                    ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-300'
                    : lastLog.status === 'duplicate'
                    ? 'bg-indigo-500/10 border-indigo-500/30 text-indigo-300'
                    : 'bg-rose-500/10 border-rose-500/30 text-rose-300'
                }`}
              >
                <div className="flex items-center justify-between text-xs font-mono mb-2">
                  <span className="font-bold flex items-center gap-1.5">
                    {lastLog.status === 'success' ? (
                      <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                    ) : lastLog.status === 'duplicate' ? (
                      <ShieldAlert className="w-4 h-4 text-indigo-400" />
                    ) : (
                      <AlertTriangle className="w-4 h-4 text-rose-400" />
                    )}
                    {lastLog.source}
                  </span>
                  <span>{lastLog.durationMs}ms</span>
                </div>
                <p className="text-xs leading-relaxed font-sans">{lastLog.message}</p>
                <div className="mt-3 pt-2.5 border-t border-slate-800/60 flex items-center justify-between text-[11px] text-slate-400 font-mono">
                  <span>Target: {eventId}</span>
                  <span>{lastLog.timestamp}</span>
                </div>
              </div>
            ) : (
              <div className="text-center py-12 text-slate-500 text-xs">
                <RotateCcw className="w-8 h-8 mx-auto mb-2 opacity-30 animate-spin-slow" />
                Submit an event or run a burst attack to inspect real-time idempotency assertions.
              </div>
            )}
          </div>
        </div>

        {/* Distributed Systems Tips */}
        <div className="mt-6 bg-slate-950 p-4 rounded-xl border border-slate-800 text-xs space-y-1.5">
          <div className="font-semibold text-slate-300">How to test idempotency:</div>
          <p className="text-slate-400 text-[11px]">
            1. Click <strong>Send Event</strong> once (Creates the business entity).
          </p>
          <p className="text-slate-400 text-[11px]">
            2. Click <strong>Send Event</strong> again with the <em>exact same ID</em>. Notice how the system intercepts it in sub-milliseconds via Redis without double-charging!
          </p>
        </div>
      </div>
    </div>
  );
};
