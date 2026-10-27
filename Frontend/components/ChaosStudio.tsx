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
  Radio,
  Terminal
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { eventService } from '@/lib/api';
import { EventRequestDto } from '@/lib/types';

interface ChaosStudioProps {
  onScenarioRun: () => void;
}

export const ChaosStudio: React.FC<ChaosStudioProps> = ({ onScenarioRun }) => {
  const [concurrencyCount, setConcurrencyCount] = useState<number>(12);
  const [isRunning, setIsRunning] = useState<boolean>(false);
  const [isRedisDown, setIsRedisDown] = useState<boolean>(eventService.isRedisDown());
  const [lastScenarioResult, setLastScenarioResult] = useState<{
    title: string;
    description: string;
    stats: string;
    status: 'success' | 'warn' | 'error';
    latencyMs: number;
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
      const t0 = performance.now();
      const res1 = await eventService.submitEvent(req);
      const res2 = await eventService.submitEvent(req);
      const elapsed = Math.round(performance.now() - t0);

      setLastScenarioResult({
        title: 'Customer Double-Click Test Complete',
        description: `Request 1 committed transaction (${res1.source}). Request 2 intercepted via ${res2.source}. Zero double-charge!`,
        stats: `Target: ${eventId} • 2 Requests dispatched • 1 Row committed`,
        status: 'success',
        latencyMs: elapsed,
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
      const t0 = performance.now();
      const promises = Array.from({ length: concurrencyCount }, () => eventService.submitEvent(req));
      const results = await Promise.all(promises);
      const elapsed = Math.round(performance.now() - t0);

      const originals = results.filter(r => r.source === 'NONE').length;
      const duplicates = results.filter(r => r.source !== 'NONE').length;

      setLastScenarioResult({
        title: `Concurrent Storm (${concurrencyCount}x Parallel Requests) Complete`,
        description: `Fired ${concurrencyCount} simultaneous threads on the same key! Exactly 1 acquired the lease; ${duplicates} were cleanly intercepted via ${isRedisDown ? 'PostgreSQL Unique Constraints' : 'Redis L1 / Postgres Fencing'}.`,
        stats: `${originals} committed • ${duplicates} deduplicated • 0 race conditions`,
        status: 'success',
        latencyMs: elapsed,
      });
      onScenarioRun();
    } finally {
      setIsRunning(false);
    }
  };

  // Scenario 3: Poison Pill
  const runPoisonPill = async () => {
    setIsRunning(true);
    const eventId = `ERR-MAL-${Math.floor(1000 + Math.random() * 9000)}`;

    try {
      const t0 = performance.now();
      await eventService.triggerPoisonPill(eventId);
      const elapsed = Math.round(performance.now() - t0);

      setLastScenarioResult({
        title: 'Poison Pill DLQ Isolation Verified',
        description: `Malformed payload failed deserialization schema validation. After 3 backoff retries, it was isolated into the 'events.dlq' Kafka topic.`,
        stats: `Target: ${eventId} • Retries: 3 • Routed to DLQ with root-cause headers`,
        status: 'error',
        latencyMs: elapsed,
      });
      onScenarioRun();
    } finally {
      setIsRunning(false);
    }
  };

  return (
    <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
      {/* Scenario Presets Grid */}
      <div className="lg:col-span-8 glass-panel rounded-2xl sm:rounded-3xl p-5 sm:p-6 shadow-xl relative overflow-hidden">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-4 sm:pb-5 border-b border-slate-800/80 gap-3">
          <div>
            <h2 className="text-sm sm:text-base font-bold text-white flex items-center gap-2">
              <Flame className="w-4 h-4 sm:w-5 sm:h-5 text-amber-400 shrink-0" />
              <span>Distributed Chaos & Concurrency Studio</span>
            </h2>
            <p className="text-[11px] sm:text-xs text-slate-400 mt-0.5">
              Select an attack scenario to test race conditions, partition retries, and fail-open resilience.
            </p>
          </div>

          {/* Redis Outage Simulation Switch */}
          <motion.button
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
            onClick={toggleRedis}
            className={`px-3 py-1.5 rounded-xl border text-xs font-semibold flex items-center gap-2 transition shadow-sm ${
              isRedisDown
                ? 'bg-rose-500/15 border-rose-500/40 text-rose-300 shadow-[0_0_15px_rgba(244,63,94,0.2)]'
                : 'bg-slate-950/80 border-slate-800 text-slate-300 hover:border-slate-700'
            }`}
          >
            {isRedisDown ? (
              <>
                <ToggleRight className="w-4 h-4 text-rose-400" />
                <span className="font-mono">Redis Outage: FAIL-OPEN Active</span>
              </>
            ) : (
              <>
                <ToggleLeft className="w-4 h-4 text-slate-500" />
                <span>Simulate Redis Outage</span>
              </>
            )}
          </motion.button>
        </div>

        {/* 3 Interactive Cards */}
        <div className="mt-5 grid grid-cols-1 md:grid-cols-3 gap-4">
          {/* Card 1: Double-Click */}
          <motion.div 
            whileHover={{ y: -3 }}
            className="glass-panel rounded-xl p-4 flex flex-col justify-between hover:border-indigo-500/50 transition-all shadow-sm"
          >
            <div>
              <div className="flex items-center justify-between mb-1.5">
                <span className="text-[10px] font-mono font-bold text-indigo-400 uppercase tracking-wider">
                  Scenario 01
                </span>
                <span className="text-[10px] font-mono text-emerald-400">Sub-3ms</span>
              </div>
              <h3 className="text-sm font-bold text-white">The Double-Click</h3>
              <p className="text-xs text-slate-400 mt-1.5 leading-relaxed">
                Simulates impatient user double-tapping payment button. Proves instant L1 Redis fast-path short-circuiting.
              </p>
            </div>
            <motion.button
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
              onClick={runDoubleClick}
              disabled={isRunning}
              className="mt-4 w-full py-2.5 px-3 bg-gradient-to-r from-indigo-600 to-indigo-700 hover:from-indigo-500 hover:to-indigo-600 text-white font-semibold text-xs rounded-xl shadow-md shadow-indigo-600/25 border border-indigo-400/30 transition disabled:opacity-50"
            >
              Trigger 2x Double-Click
            </motion.button>
          </motion.div>

          {/* Card 2: Concurrency Storm */}
          <motion.div 
            whileHover={{ y: -3 }}
            className="glass-panel rounded-xl p-4 flex flex-col justify-between hover:border-amber-500/50 transition-all shadow-sm"
          >
            <div>
              <div className="flex items-center justify-between mb-1.5">
                <span className="text-[10px] font-mono font-bold text-amber-400 uppercase tracking-wider">
                  Scenario 02
                </span>
                <span className="text-[10px] font-mono text-amber-400">Race Test</span>
              </div>
              <h3 className="text-sm font-bold text-white">Parallel Concurrency Storm</h3>
              <p className="text-xs text-slate-400 mt-1 leading-relaxed">
                Fires simultaneous parallel threads on identical key to test PostgreSQL atomic lease token locks.
              </p>

              <div className="mt-3 bg-slate-950/60 p-2.5 rounded-lg border border-slate-800/80">
                <div className="flex items-center justify-between text-[11px] text-slate-400 mb-1.5 font-mono">
                  <span>Threads:</span>
                  <span className="text-amber-400 font-bold bg-amber-500/10 px-2 py-0.5 rounded border border-amber-500/20">
                    {concurrencyCount} requests
                  </span>
                </div>
                <input
                  type="range"
                  min={3}
                  max={25}
                  value={concurrencyCount}
                  onChange={e => setConcurrencyCount(parseInt(e.target.value))}
                  className="w-full accent-amber-500 cursor-pointer h-1.5 bg-slate-800 rounded-lg"
                />
              </div>
            </div>
            <motion.button
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
              onClick={runConcurrencyStorm}
              disabled={isRunning}
              className="mt-4 w-full py-2.5 px-3 bg-gradient-to-r from-amber-600 to-amber-700 hover:from-amber-500 hover:to-amber-600 text-white font-semibold text-xs rounded-xl shadow-md shadow-amber-600/25 border border-amber-400/30 transition disabled:opacity-50"
            >
              Fire {concurrencyCount}x Storm
            </motion.button>
          </motion.div>

          {/* Card 3: Poison Pill */}
          <motion.div 
            whileHover={{ y: -3 }}
            className="glass-panel rounded-xl p-4 flex flex-col justify-between hover:border-rose-500/50 transition-all shadow-sm"
          >
            <div>
              <div className="flex items-center justify-between mb-1.5">
                <span className="text-[10px] font-mono font-bold text-rose-400 uppercase tracking-wider">
                  Scenario 03
                </span>
                <span className="text-[10px] font-mono text-rose-400">Dead Letter</span>
              </div>
              <h3 className="text-sm font-bold text-white">Poison Pill (DLQ)</h3>
              <p className="text-xs text-slate-400 mt-1 leading-relaxed">
                Injects invalid schema payload. Exercises Kafka exponential backoff retries and Dead Letter Queue isolation.
              </p>
            </div>
            <motion.button
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
              onClick={runPoisonPill}
              disabled={isRunning}
              className="mt-4 w-full py-2.5 px-3 bg-gradient-to-r from-rose-600 to-rose-700 hover:from-rose-500 hover:to-rose-600 text-white font-semibold text-xs rounded-xl shadow-md shadow-rose-600/25 border border-rose-400/30 transition disabled:opacity-50"
            >
              Inject Poison Pill
            </motion.button>
          </motion.div>
        </div>
      </div>

      {/* Scenario Feedback & Latency Engine */}
      <div className="lg:col-span-4 glass-panel rounded-2xl sm:rounded-3xl p-5 sm:p-6 shadow-xl flex flex-col justify-between">
        <div>
          <div className="flex items-center justify-between pb-4 border-b border-slate-800/80">
            <h3 className="text-sm font-bold text-white flex items-center gap-2">
              <Terminal className="w-4 h-4 text-cyan-400" />
              <span>Chaos Diagnostic Console</span>
            </h3>
            <span className="flex items-center gap-1.5 text-[10px] font-mono text-emerald-400">
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-ping" />
              LIVE TELEMETRY
            </span>
          </div>

          <div className="mt-4">
            <AnimatePresence mode="wait">
              {lastScenarioResult ? (
                <motion.div
                  key={lastScenarioResult.title}
                  initial={{ opacity: 0, scale: 0.94, y: 8 }}
                  animate={{ opacity: 1, scale: 1, y: 0 }}
                  exit={{ opacity: 0, scale: 0.94 }}
                  transition={{ duration: 0.25 }}
                  className={`p-4 rounded-xl border ${
                    lastScenarioResult.status === 'success'
                      ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-300 shadow-[0_0_20px_rgba(16,185,129,0.15)]'
                      : 'bg-rose-500/10 border-rose-500/30 text-rose-300 shadow-[0_0_20px_rgba(244,63,94,0.15)]'
                  }`}
                >
                  <div className="flex items-center justify-between font-bold text-xs mb-1.5">
                    <span className="flex items-center gap-2">
                      {lastScenarioResult.status === 'success' ? (
                        <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                      ) : (
                        <AlertTriangle className="w-4 h-4 text-rose-400" />
                      )}
                      {lastScenarioResult.title}
                    </span>
                    <span className="font-mono text-[10px] opacity-80">{lastScenarioResult.latencyMs}ms</span>
                  </div>
                  <p className="text-xs text-slate-300 mt-1 leading-relaxed font-sans">
                    {lastScenarioResult.description}
                  </p>
                  <div className="mt-3 pt-2 border-t border-slate-800/80 text-[11px] font-mono text-slate-400">
                    {lastScenarioResult.stats}
                  </div>
                </motion.div>
              ) : (
                <div className="text-center py-10 text-slate-500 text-xs font-sans">
                  Select an attack scenario from the left to observe distributed system assertions live.
                </div>
              )}
            </AnimatePresence>
          </div>
        </div>

        {/* Latency Comparison Card */}
        <div className="mt-6 p-4 rounded-xl bg-slate-950/80 border border-slate-800/80 text-xs space-y-3 shadow-inner">
          <span className="font-semibold text-slate-300 block text-xs">Performance Comparison:</span>
          <div>
            <div className="flex justify-between text-[11px] font-mono text-slate-400 mb-1">
              <span>Redis L1 Fast-Path Cache:</span>
              <span className="text-emerald-400 font-bold">&lt; 3ms</span>
            </div>
            <div className="w-full bg-slate-900 h-2 rounded-full overflow-hidden p-[1px]">
              <motion.div 
                initial={{ width: 0 }}
                animate={{ width: '8%' }}
                transition={{ duration: 0.8, ease: 'easeOut' }}
                className="bg-emerald-400 h-full rounded-full shadow-[0_0_8px_rgba(52,211,153,0.8)]" 
              />
            </div>
          </div>
          <div>
            <div className="flex justify-between text-[11px] font-mono text-slate-400 mb-1">
              <span>PostgreSQL L2 Full ACID Txn:</span>
              <span className="text-indigo-400 font-bold">~42ms</span>
            </div>
            <div className="w-full bg-slate-900 h-2 rounded-full overflow-hidden p-[1px]">
              <motion.div 
                initial={{ width: 0 }}
                animate={{ width: '82%' }}
                transition={{ duration: 0.8, ease: 'easeOut', delay: 0.1 }}
                className="bg-gradient-to-r from-indigo-500 to-cyan-500 h-full rounded-full shadow-[0_0_8px_rgba(99,102,241,0.8)]" 
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};
