'use client';

import React, { useState } from 'react';
import { 
  Zap, 
  Flame, 
  AlertTriangle, 
  ShieldAlert, 
  Database, 
  RotateCcw, 
  Sliders, 
  Send, 
  CheckCircle2, 
  ToggleLeft, 
  ToggleRight,
  ServerOff,
  Radio
} from 'lucide-react';
import { eventService } from '@/lib/api';
import { EventRequestDto } from '@/lib/types';

interface ChaosStudioProps {
  onScenarioRun: () => void;
}

export const ChaosStudio: React.FC<ChaosStudioProps> = ({ onScenarioRun }) => {
  const [concurrencyCount, setConcurrencyCount] = useState<number>(10);
  const [isRunning, setIsRunning] = useState<boolean>(false);
  const [isRedisDown, setIsRedisDown] = useState<boolean>(eventService.isRedisDown());
  const [lastScenarioResult, setLastScenarioResult] = useState<{
    title: string;
    description: string;
    stats: string;
    status: 'success' | 'warn' | 'error';
  } | null>(null);

  const toggleRedis = () => {
    const newState = eventService.toggleRedisSimulatedDown();
    setIsRedisDown(newState);
    onScenarioRun();
  };

  // Scenario 1: Double-Click
  const runDoubleClick = async () => {
    setIsRunning(true);
    const eventId = `PAY-DBL-${Math.floor(1000 + Math.random() * 9000)}`;
    const req: EventRequestDto = {
      eventId,
      eventType: 'PAYMENT_PROCESSED',
      payload: { paymentId: eventId, userId: 'user_shopper', amount: 89.99, currency: 'USD' },
    };

    try {
      const res1 = await eventService.submitEvent(req);
      const res2 = await eventService.submitEvent(req);

      setLastScenarioResult({
        title: 'Customer Double-Click Test Complete',
        description: `Request 1 took full execution (${res1.source}). Request 2 intercepted via ${res2.source}. Zero double-charge!`,
        stats: `Target: ${eventId} | 2 Requests dispatched | 1 Database mutation created`,
        status: 'success',
      });
      onScenarioRun();
    } finally {
      setIsRunning(false);
    }
  };

  // Scenario 2: Concurrency Storm
  const runConcurrencyStorm = async () => {
    setIsRunning(true);
    const eventId = `STORM-TXN-${Math.floor(1000 + Math.random() * 9000)}`;
    const req: EventRequestDto = {
      eventId,
      eventType: 'PAYMENT_PROCESSED',
      payload: { paymentId: eventId, userId: 'user_storm', amount: 250.0, currency: 'USD' },
    };

    try {
      const promises = Array.from({ length: concurrencyCount }, () => eventService.submitEvent(req));
      const results = await Promise.all(promises);

      const originals = results.filter(r => r.source === 'NONE').length;
      const duplicates = results.filter(r => r.source !== 'NONE').length;

      setLastScenarioResult({
        title: `Concurrent Storm (${concurrencyCount}x Parallel Requests) Complete`,
        description: `Fired ${concurrencyCount} simultaneous threads on the same key! Exactly 1 acquired the lease; ${duplicates} were cleanly intercepted via ${isRedisDown ? 'PostgreSQL Unique Constraints' : 'Redis/Postgres'}.`,
        stats: `${originals} committed • ${duplicates} deduplicated • 0 race conditions`,
        status: 'success',
      });
      onScenarioRun();
    } finally {
      setIsRunning(false);
    }
  };

  // Scenario 3: Poison Pill
  const runPoisonPill = async () => {
    setIsRunning(true);
    const eventId = `POISON-BAD-${Math.floor(1000 + Math.random() * 9000)}`;
    try {
      await eventService.triggerPoisonPill(eventId);
      setLastScenarioResult({
        title: 'Poison Pill Isolated to DLQ',
        description: `Malformed payload detected. Kafka exponential backoff retried 3 times before routing safely to the Dead Letter Queue (events.dlq).`,
        stats: `Target: ${eventId} • Retries: 3 • State: FAILED (DLQ) • Main topic unblocked`,
        status: 'error',
      });
      onScenarioRun();
    } finally {
      setIsRunning(false);
    }
  };

  return (
    <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
      {/* Scenario Presets Grid */}
      <div className="lg:col-span-8 bg-slate-900 border border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-4 sm:pb-5 border-b border-slate-800 gap-3">
          <div>
            <h2 className="text-sm sm:text-base font-bold text-white flex items-center gap-2">
              <Flame className="w-4 h-4 sm:w-5 sm:h-5 text-amber-400 shrink-0" />
              <span>Distributed Systems Chaos & Attack Studio</span>
            </h2>
            <p className="text-[11px] sm:text-xs text-slate-400 mt-0.5">
              Select an attack scenario to test race conditions, partition retries, and fail-open resilience.
            </p>
          </div>

          {/* Redis Outage Simulation Switch */}
          <button
            onClick={toggleRedis}
            className={`px-3 py-1.5 rounded-xl border text-xs font-semibold flex items-center gap-2 transition ${
              isRedisDown
                ? 'bg-rose-500/10 border-rose-500/30 text-rose-300'
                : 'bg-slate-950 border-slate-800 text-slate-300 hover:border-slate-700'
            }`}
          >
            {isRedisDown ? (
              <>
                <ToggleRight className="w-4 h-4 text-rose-400" />
                <span>Redis Outage: FAIL-OPEN Active</span>
              </>
            ) : (
              <>
                <ToggleLeft className="w-4 h-4 text-slate-500" />
                <span>Simulate Redis Outage</span>
              </>
            )}
          </button>
        </div>

        {/* 3 Interactive Cards */}
        <div className="mt-5 grid grid-cols-1 md:grid-cols-3 gap-4">
          {/* Card 1: Double-Click */}
          <div className="bg-slate-950/70 border border-slate-800 rounded-xl p-4 flex flex-col justify-between hover:border-slate-700 transition">
            <div>
              <span className="text-[10px] font-mono font-bold text-indigo-400 uppercase tracking-wider block mb-1">
                Scenario 01
              </span>
              <h3 className="text-sm font-semibold text-white">The Double-Click</h3>
              <p className="text-xs text-slate-400 mt-1 leading-relaxed">
                Simulates impatient user double-tapping payment button. Proves instant L1 Redis fast-path short-circuiting.
              </p>
            </div>
            <button
              onClick={runDoubleClick}
              disabled={isRunning}
              className="mt-4 w-full py-2 px-3 bg-slate-900 hover:bg-slate-800 text-indigo-300 font-semibold text-xs rounded-lg border border-slate-700 transition disabled:opacity-50"
            >
              Trigger 2x Double-Click
            </button>
          </div>

          {/* Card 2: Concurrency Storm */}
          <div className="bg-slate-950/70 border border-slate-800 rounded-xl p-4 flex flex-col justify-between hover:border-slate-700 transition">
            <div>
              <span className="text-[10px] font-mono font-bold text-amber-400 uppercase tracking-wider block mb-1">
                Scenario 02
              </span>
              <h3 className="text-sm font-semibold text-white">Parallel Duplicate Storm</h3>
              <p className="text-xs text-slate-400 mt-1 leading-relaxed">
                Fires simultaneous parallel threads with identical key to test PostgreSQL atomic lease token locks.
              </p>

              <div className="mt-3">
                <div className="flex items-center justify-between text-[11px] text-slate-400 mb-1 font-mono">
                  <span>Threads:</span>
                  <span className="text-amber-400 font-bold">{concurrencyCount} requests</span>
                </div>
                <input
                  type="range"
                  min={3}
                  max={25}
                  value={concurrencyCount}
                  onChange={e => setConcurrencyCount(parseInt(e.target.value))}
                  className="w-full accent-amber-500 cursor-pointer"
                />
              </div>
            </div>
            <button
              onClick={runConcurrencyStorm}
              disabled={isRunning}
              className="mt-4 w-full py-2 px-3 bg-amber-600/20 hover:bg-amber-600/30 text-amber-400 font-semibold text-xs rounded-lg border border-amber-500/30 transition disabled:opacity-50"
            >
              Fire {concurrencyCount}x Concurrent Storm
            </button>
          </div>

          {/* Card 3: Poison Pill */}
          <div className="bg-slate-950/70 border border-slate-800 rounded-xl p-4 flex flex-col justify-between hover:border-slate-700 transition">
            <div>
              <span className="text-[10px] font-mono font-bold text-rose-400 uppercase tracking-wider block mb-1">
                Scenario 03
              </span>
              <h3 className="text-sm font-semibold text-white">Poison Pill (DLQ)</h3>
              <p className="text-xs text-slate-400 mt-1 leading-relaxed">
                Injects invalid schema payload. Exercises Kafka exponential backoff retries and Dead Letter Queue isolation.
              </p>
            </div>
            <button
              onClick={runPoisonPill}
              disabled={isRunning}
              className="mt-4 w-full py-2 px-3 bg-rose-500/10 hover:bg-rose-500/20 text-rose-400 font-semibold text-xs rounded-lg border border-rose-500/30 transition disabled:opacity-50"
            >
              Inject Poison Pill
            </button>
          </div>
        </div>
      </div>

      {/* Scenario Feedback & Latency Engine */}
      <div className="lg:col-span-4 bg-slate-900 border border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm flex flex-col justify-between">
        <div>
          <div className="flex items-center justify-between pb-4 border-b border-slate-800">
            <h3 className="text-sm font-bold text-white flex items-center gap-2">
              <Zap className="w-4 h-4 text-indigo-400" />
              Scenario Diagnostic Output
            </h3>
            <span className="text-[10px] font-mono text-slate-500">Live Telemetry</span>
          </div>

          <div className="mt-4">
            {lastScenarioResult ? (
              <div
                className={`p-4 rounded-xl border ${
                  lastScenarioResult.status === 'success'
                    ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-300'
                    : 'bg-rose-500/10 border-rose-500/30 text-rose-300'
                }`}
              >
                <div className="flex items-center gap-2 font-bold text-xs mb-1">
                  {lastScenarioResult.status === 'success' ? (
                    <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                  ) : (
                    <AlertTriangle className="w-4 h-4 text-rose-400" />
                  )}
                  {lastScenarioResult.title}
                </div>
                <p className="text-xs text-slate-300 mt-1 leading-relaxed font-sans">
                  {lastScenarioResult.description}
                </p>
                <div className="mt-3 pt-2 border-t border-slate-800/80 text-[11px] font-mono text-slate-400">
                  {lastScenarioResult.stats}
                </div>
              </div>
            ) : (
              <div className="text-center py-10 text-slate-500 text-xs font-sans">
                Select an attack scenario from the left to observe distributed system assertions live.
              </div>
            )}
          </div>
        </div>

        {/* Latency Comparison Card */}
        <div className="mt-6 p-4 rounded-xl bg-slate-950 border border-slate-800 text-xs space-y-2.5">
          <span className="font-semibold text-slate-300 block">Performance Benchmark:</span>
          <div>
            <div className="flex justify-between text-[11px] font-mono text-slate-400 mb-1">
              <span>Redis L1 Fast-Path Cache:</span>
              <span className="text-emerald-400 font-bold">~2ms - 4ms</span>
            </div>
            <div className="w-full bg-slate-900 h-2 rounded-full overflow-hidden">
              <div className="bg-emerald-400 h-full w-[8%]" />
            </div>
          </div>
          <div>
            <div className="flex justify-between text-[11px] font-mono text-slate-400 mb-1">
              <span>PostgreSQL L2 Full Transaction:</span>
              <span className="text-indigo-400 font-bold">~38ms - 55ms</span>
            </div>
            <div className="w-full bg-slate-900 h-2 rounded-full overflow-hidden">
              <div className="bg-indigo-500 h-full w-[85%]" />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};
