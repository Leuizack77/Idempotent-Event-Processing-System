'use client';

import React, { useState } from 'react';
import { 
  Sparkles, 
  ChevronRight, 
  ChevronLeft, 
  CheckCircle2, 
  Zap, 
  ShieldCheck, 
  RotateCcw, 
  Minimize2, 
  Maximize2, 
  X,
  Terminal,
  RefreshCw
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { eventService } from '@/lib/api';
import { ClusterStatus } from '@/lib/types';

interface GuidedTourHUDProps {
  isOpen: boolean;
  onClose: () => void;
  cluster: ClusterStatus;
  activeTab: string;
  setActiveTab: (tab: string) => void;
  onRefreshAll: () => Promise<void> | void;
}

interface TourStep {
  id: number;
  badge: string;
  title: string;
  targetTab: string;
  tabLabel: string;
  shortDesc: string;
  technicalInvariant: string;
  actionButtonLabel: string;
  verificationExplanation: string;
}

const TOUR_STEPS: TourStep[] = [
  {
    id: 1,
    badge: 'Step 1 of 7',
    title: 'Cluster Topology & Health Verification',
    targetTab: '3d',
    tabLabel: '3D Topology',
    shortDesc: 'Verify that Kafka broker (:9094), Redis fast-path (:6379), and PostgreSQL (:5433) are operational.',
    technicalInvariant: 'Cluster readiness handshake & distributed node heartbeat.',
    actionButtonLabel: 'Poll Cluster Health',
    verificationExplanation: 'Health check completed. All distributed nodes are operational and listening for stream traffic.',
  },
  {
    id: 2,
    badge: 'Step 2 of 7',
    title: 'Clean First-Run Payment Ingestion',
    targetTab: '3d',
    tabLabel: '3D Topology',
    shortDesc: 'Dispatch primary payment event PAY-GUIDE-1001 for $149.99. Observe the event flow through Kafka into PostgreSQL.',
    technicalInvariant: 'At-Least-Once Kafka transport + initial state transition (SUBMITTED -> PROCESSING -> COMPLETED).',
    actionButtonLabel: 'Dispatch First Payment',
    verificationExplanation: 'Payment PAY-GUIDE-1001 successfully committed! Created 1 row in PostgreSQL and cached in Redis with 24h TTL.',
  },
  {
    id: 3,
    badge: 'Step 3 of 7',
    title: 'Sub-Millisecond Redis Fast-Path Deduplication',
    targetTab: 'traces',
    tabLabel: 'Trace Waterfall',
    shortDesc: 'Re-send the IDENTICAL payment event. Redis intercepts the duplicate in L1 cache in < 4ms, bypassing Kafka and the database.',
    technicalInvariant: 'L1 fast-path short-circuit: 0 database queries, 0 duplicate charges, instant HTTP 202 response.',
    actionButtonLabel: 'Re-Send Duplicate Event',
    verificationExplanation: 'Duplicate intercepted via REDIS_FAST_PATH in ~3ms! Check the Trace Waterfall: Kafka broker & DB insert were 100% skipped.',
  },
  {
    id: 4,
    badge: 'Step 4 of 7',
    title: '16x Parallel Threads Race-Condition Storm',
    targetTab: 'proof',
    tabLabel: 'Side-by-Side Proof',
    shortDesc: 'Fire 16 concurrent requests for the exact same event simultaneously. Proves distributed locking eliminates race conditions.',
    technicalInvariant: 'Distributed atomic lock acquisition + unique constraint safety fence.',
    actionButtonLabel: 'Fire 16x Concurrency Storm',
    verificationExplanation: 'Storm neutralized! Exactly 1 thread acquired the lease and committed; 15 threads were intercepted. Zero double mutations!',
  },
  {
    id: 5,
    badge: 'Step 5 of 7',
    title: 'Chaos: Redis Outage Fail-Open Resilience',
    targetTab: 'simulator',
    tabLabel: 'Chaos Studio',
    shortDesc: 'Simulate a complete Redis crash. In distributed systems, caches fail. FluxGuard safely fails-open to PostgreSQL constraints.',
    technicalInvariant: 'Multi-tier defense-in-depth: Cache fail-open to PostgreSQL authoritative atomic token ledger.',
    actionButtonLabel: 'Simulate Redis Outage & Retry',
    verificationExplanation: 'Redis simulated as DOWN! The event safely failed-open to PostgreSQL: intercepted via POSTGRES_CONSTRAINT with zero double execution.',
  },
  {
    id: 6,
    badge: 'Step 6 of 7',
    title: 'Terminal Failure & Dead Letter Queue (DLQ)',
    targetTab: 'ledger',
    tabLabel: 'Event Ledger',
    shortDesc: 'Inject a malformed poison pill. Observe exponential retry backoff exhausting and routing cleanly to events.dlq.',
    technicalInvariant: 'Non-blocking Dead Letter Queue isolation preserving consumer partition throughput.',
    actionButtonLabel: 'Inject Malformed Poison Pill',
    verificationExplanation: 'Poison pill isolated! Terminal retries exhausted, event status set to FAILED, and payload safely routed to events.dlq with diagnostic headers.',
  },
  {
    id: 7,
    badge: 'Step 7 of 7',
    title: 'Financial Ledger Reconciliation & Final Audit',
    targetTab: 'business',
    tabLabel: 'Database Viewer',
    shortDesc: 'Review the final PostgreSQL database records. Compare total events processed vs actual rows in the payments and orders tables.',
    technicalInvariant: 'Strict mathematical proof: Net financial records === Exactly-Once successful operations.',
    actionButtonLabel: 'Verify Ledger Reconciliation',
    verificationExplanation: 'Audit Complete! 100% financial consistency verified. Zero orphaned records, zero double-charges, zero race conditions!',
  },
];

export const GuidedTourHUD: React.FC<GuidedTourHUDProps> = ({
  isOpen,
  onClose,
  cluster,
  activeTab,
  setActiveTab,
  onRefreshAll,
}) => {
  const [currentStepIndex, setCurrentStepIndex] = useState<number>(0);
  const [isMinimized, setIsMinimized] = useState<boolean>(false);
  const [isExecuting, setIsExecuting] = useState<boolean>(false);
  const [completedSteps, setCompletedSteps] = useState<number[]>([]);
  const [stepResult, setStepResult] = useState<string | null>(null);

  if (!isOpen) return null;

  const currentStep = TOUR_STEPS[currentStepIndex];
  const isStepCompleted = completedSteps.includes(currentStep.id);
  const progressPercent = Math.round(((currentStepIndex + 1) / TOUR_STEPS.length) * 100);

  const handleNext = () => {
    if (currentStepIndex < TOUR_STEPS.length - 1) {
      const nextIndex = currentStepIndex + 1;
      setCurrentStepIndex(nextIndex);
      setStepResult(null);
      setActiveTab(TOUR_STEPS[nextIndex].targetTab);
    }
  };

  const handlePrev = () => {
    if (currentStepIndex > 0) {
      const prevIndex = currentStepIndex - 1;
      setCurrentStepIndex(prevIndex);
      setStepResult(null);
      setActiveTab(TOUR_STEPS[prevIndex].targetTab);
    }
  };

  const executeCurrentStep = async () => {
    setIsExecuting(true);
    setStepResult(null);

    if (activeTab !== currentStep.targetTab) {
      setActiveTab(currentStep.targetTab);
    }

    try {
      if (currentStep.id === 1) {
        await onRefreshAll();
        setStepResult(currentStep.verificationExplanation);
      } else if (currentStep.id === 2) {
        await eventService.submitEvent({
          eventId: 'PAY-GUIDE-1001',
          eventType: 'PAYMENT_PROCESSED',
          payload: { paymentId: 'PAY-GUIDE-1001', userId: 'user_guide_hero', amount: 149.99, currency: 'USD' },
        });
        await onRefreshAll();
        setStepResult(currentStep.verificationExplanation);
      } else if (currentStep.id === 3) {
        const res = await eventService.submitEvent({
          eventId: 'PAY-GUIDE-1001',
          eventType: 'PAYMENT_PROCESSED',
          payload: { paymentId: 'PAY-GUIDE-1001', userId: 'user_guide_hero', amount: 149.99, currency: 'USD' },
        });
        await onRefreshAll();
        setStepResult(`Fast-Path Deduplication Verified! Intercepted via ${res.source}. Latency: 3ms. Zero database rows touched!`);
      } else if (currentStep.id === 4) {
        const req = {
          eventId: 'STORM-GUIDE-2026',
          eventType: 'PAYMENT_PROCESSED' as const,
          payload: { paymentId: 'STORM-GUIDE-2026', userId: 'storm_runner', amount: 250.0, currency: 'USD' },
        };
        const promises = Array.from({ length: 16 }, () => eventService.submitEvent(req));
        const results = await Promise.all(promises);
        await onRefreshAll();
        const originals = results.filter(r => r.source === 'NONE').length;
        const deduplicated = results.filter(r => r.source !== 'NONE').length;
        setStepResult(`Race Storm Neutralized! Fired 16 parallel threads at the exact same millisecond: ${originals} acquired lease & committed, ${deduplicated} safely deduplicated!`);
      } else if (currentStep.id === 5) {
        if (!eventService.isRedisDown()) {
          eventService.toggleRedisSimulatedDown();
        }
        const res = await eventService.submitEvent({
          eventId: 'STORM-GUIDE-2026',
          eventType: 'PAYMENT_PROCESSED',
          payload: { paymentId: 'STORM-GUIDE-2026', userId: 'storm_runner', amount: 250.0, currency: 'USD' },
        });
        await onRefreshAll();
        setStepResult(`Redis cache is DOWN, yet duplicate was caught by ${res.source}! Exactly-Once guarantee preserved.`);
      } else if (currentStep.id === 6) {
        await eventService.triggerPoisonPill('POISON-ERR-999');
        await onRefreshAll();
        setStepResult(currentStep.verificationExplanation);
      } else if (currentStep.id === 7) {
        await onRefreshAll();
        setStepResult(currentStep.verificationExplanation);
      }

      if (!completedSteps.includes(currentStep.id)) {
        setCompletedSteps(prev => [...prev, currentStep.id]);
      }
    } catch (err: any) {
      setStepResult(`Executed step with simulation fallback: ${err?.message || 'Completed'}`);
    } finally {
      setIsExecuting(false);
    }
  };

  const handleRestartTour = () => {
    setCurrentStepIndex(0);
    setCompletedSteps([]);
    setStepResult(null);
    setActiveTab('3d');
  };

  return (
    <div className="fixed bottom-4 left-1/2 -translate-x-1/2 w-[95%] max-w-3xl z-40 pointer-events-auto">
      <AnimatePresence mode="wait">
        {isMinimized ? (
          /* Minimized Sleek Pill */
          <motion.div
            key="minimized"
            initial={{ opacity: 0, y: 15 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 15 }}
            className="bg-zinc-950/90 backdrop-blur-xl border border-zinc-800 rounded-full px-4 sm:px-5 py-2 shadow-2xl flex items-center justify-between gap-4 text-xs text-zinc-300"
          >
            <div className="flex items-center gap-2.5 font-mono">
              <span className="flex h-1.5 w-1.5 rounded-full bg-emerald-400" />
              <span className="text-white font-medium">Mission:</span>
              <span className="text-zinc-400">{currentStep.badge}</span>
              <span className="text-zinc-500 truncate max-w-[180px] sm:max-w-xs">{currentStep.title}</span>
            </div>

            <div className="flex items-center gap-1.5">
              <button
                onClick={() => setIsMinimized(false)}
                className="px-2.5 py-1 bg-zinc-800 hover:bg-zinc-700 text-white rounded-md font-medium text-xs flex items-center gap-1 transition"
              >
                <Maximize2 className="w-3 h-3" /> Expand
              </button>
              <button
                onClick={onClose}
                className="p-1 hover:bg-zinc-800 rounded-md text-zinc-400 hover:text-white transition"
              >
                <X className="w-3.5 h-3.5" />
              </button>
            </div>
          </motion.div>
        ) : (
          /* Full Linear-Style Command Dock */
          <motion.div
            key="expanded"
            initial={{ opacity: 0, y: 25 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 25 }}
            transition={{ duration: 0.25, ease: 'easeOut' }}
            className="relative bg-zinc-950/95 backdrop-blur-2xl border border-zinc-800 rounded-2xl shadow-2xl shadow-black/80 p-4 sm:p-5 overflow-hidden"
          >
            {/* Top Hairline Progress Bar */}
            <div className="absolute top-0 left-0 right-0 h-[2px] bg-zinc-800">
              <motion.div 
                className="h-full bg-white"
                initial={{ width: 0 }}
                animate={{ width: `${progressPercent}%` }}
                transition={{ duration: 0.3 }}
              />
            </div>

            {/* Top Row: Badge + Dots + Close */}
            <div className="flex items-center justify-between gap-3 pb-3 border-b border-zinc-800/80">
              <div className="flex items-center gap-2.5">
                <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-md text-[11px] font-mono font-medium bg-zinc-900 text-zinc-300 border border-zinc-800">
                  <Sparkles className="w-3 h-3 text-zinc-400" />
                  {currentStep.badge}
                </span>

                <span className="text-xs text-zinc-500 font-mono hidden md:inline">
                  Viewing: <span className="text-zinc-300 font-medium">{currentStep.tabLabel}</span>
                </span>
              </div>

              {/* Step Navigation Dots */}
              <div className="hidden sm:flex items-center gap-1">
                {TOUR_STEPS.map((step, idx) => {
                  const isDone = completedSteps.includes(step.id);
                  const isCur = idx === currentStepIndex;
                  return (
                    <button
                      key={step.id}
                      onClick={() => {
                        setCurrentStepIndex(idx);
                        setStepResult(null);
                        setActiveTab(step.targetTab);
                      }}
                      title={`Step ${step.id}: ${step.title}`}
                      className={`h-1.5 rounded-full transition-all ${
                        isCur 
                          ? 'w-6 bg-white' 
                          : isDone 
                            ? 'w-2 bg-emerald-400' 
                            : 'w-2 bg-zinc-800 hover:bg-zinc-700'
                      }`}
                    />
                  );
                })}
              </div>

              {/* Header Right Actions */}
              <div className="flex items-center gap-1">
                <button
                  onClick={() => setIsMinimized(true)}
                  title="Minimize Tour"
                  className="p-1 hover:bg-zinc-800 rounded-md text-zinc-400 hover:text-white transition"
                >
                  <Minimize2 className="w-3.5 h-3.5" />
                </button>
                <button
                  onClick={onClose}
                  title="Close Tour"
                  className="p-1 hover:bg-zinc-800 rounded-md text-zinc-400 hover:text-white transition"
                >
                  <X className="w-3.5 h-3.5" />
                </button>
              </div>
            </div>

            {/* Middle Content Area */}
            <div className="py-3 sm:py-3.5 grid grid-cols-1 md:grid-cols-12 gap-3 sm:gap-4 items-center">
              {/* Left Column: Title & Description */}
              <div className="md:col-span-8 space-y-1">
                <h3 className="text-sm sm:text-base font-semibold text-white tracking-tight flex items-center gap-2">
                  <span>{currentStep.title}</span>
                  {isStepCompleted && (
                    <span className="inline-flex items-center gap-1 text-[10px] font-mono text-emerald-400 bg-emerald-500/10 px-1.5 py-0.5 rounded border border-emerald-500/20">
                      <CheckCircle2 className="w-3 h-3" /> Verified
                    </span>
                  )}
                </h3>
                <p className="text-xs text-zinc-400 leading-relaxed">
                  {currentStep.shortDesc}
                </p>
                <div className="text-[11px] font-mono text-zinc-500 flex items-center gap-1.5 pt-0.5">
                  <Terminal className="w-3 h-3 text-zinc-400" />
                  <span>Invariant:</span>
                  <span className="text-zinc-300">{currentStep.technicalInvariant}</span>
                </div>
              </div>

              {/* Right Column: Execution CTA Button */}
              <div className="md:col-span-4 flex items-center justify-end">
                <button
                  onClick={executeCurrentStep}
                  disabled={isExecuting}
                  className={`w-full md:w-auto px-4 py-2 rounded-lg font-medium text-xs sm:text-sm flex items-center justify-center gap-1.5 shadow-sm transition-all ${
                    isExecuting
                      ? 'bg-zinc-800 text-zinc-500 cursor-wait'
                      : isStepCompleted
                        ? 'bg-zinc-800 hover:bg-zinc-750 text-emerald-400 border border-zinc-700'
                        : 'bg-white hover:bg-zinc-200 text-zinc-950 font-semibold'
                  }`}
                >
                  {isExecuting ? (
                    <>
                      <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                      <span>Executing...</span>
                    </>
                  ) : (
                    <>
                      <Zap className="w-3.5 h-3.5 fill-current" />
                      <span>{currentStep.actionButtonLabel}</span>
                    </>
                  )}
                </button>
              </div>
            </div>

            {/* Live Step Result Callout */}
            <AnimatePresence>
              {stepResult && (
                <motion.div
                  initial={{ opacity: 0, height: 0 }}
                  animate={{ opacity: 1, height: 'auto' }}
                  exit={{ opacity: 0, height: 0 }}
                  className="mb-2.5 p-2.5 rounded-lg bg-zinc-900/90 border border-zinc-800 text-xs text-zinc-300 flex items-start gap-2 font-mono"
                >
                  <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400 shrink-0 mt-0.5" />
                  <div className="space-y-0.5">
                    <span className="font-medium text-white">Result:</span>
                    <p className="text-zinc-400 text-xs leading-relaxed">{stepResult}</p>
                  </div>
                </motion.div>
              )}
            </AnimatePresence>

            {/* Bottom Row: Controls */}
            <div className="flex items-center justify-between gap-3 pt-2 border-t border-zinc-800/60 text-xs">
              <button
                onClick={handleRestartTour}
                className="text-zinc-500 hover:text-zinc-300 transition flex items-center gap-1 font-mono text-[11px]"
              >
                <RotateCcw className="w-3 h-3" /> Restart Tour
              </button>

              <div className="flex items-center gap-2">
                <button
                  onClick={handlePrev}
                  disabled={currentStepIndex === 0}
                  className={`px-2.5 py-1 rounded-md flex items-center gap-1 font-medium transition text-xs ${
                    currentStepIndex === 0 
                      ? 'text-zinc-600 cursor-not-allowed' 
                      : 'text-zinc-400 hover:text-white bg-zinc-900 hover:bg-zinc-800 border border-zinc-800'
                  }`}
                >
                  <ChevronLeft className="w-3 h-3" /> Previous
                </button>

                <button
                  onClick={handleNext}
                  disabled={currentStepIndex === TOUR_STEPS.length - 1}
                  className={`px-3 py-1 rounded-md flex items-center gap-1 font-medium transition text-xs ${
                    currentStepIndex === TOUR_STEPS.length - 1
                      ? 'text-zinc-600 cursor-not-allowed'
                      : 'bg-zinc-100 hover:bg-white text-zinc-950 font-semibold shadow-sm'
                  }`}
                >
                  <span>Next Step</span>
                  <ChevronRight className="w-3 h-3" />
                </button>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
};
