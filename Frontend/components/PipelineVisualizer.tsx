'use client';

import React, { useState } from 'react';
import { ArrowRight, CheckCircle2, Database, Layers, Radio, Shield, Zap, AlertOctagon, HelpCircle } from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';

export const PipelineVisualizer: React.FC = () => {
  const [activeStep, setActiveStep] = useState<number | null>(null);

  const steps = [
    {
      id: 1,
      title: 'HTTP REST Ingestion',
      subtitle: 'POST /api/events',
      desc: 'Client submits event. Controller validates payload and synchronously inserts a SUBMITTED record into PostgreSQL before producing to Kafka.',
      tag: 'Synchronous Boundary',
      icon: Radio,
      badgeColor: 'border-blue-500/30 text-blue-400 bg-blue-500/10',
    },
    {
      id: 2,
      title: 'Apache Kafka Transport',
      subtitle: 'Topic: "events"',
      desc: 'Producer publishes event with Key=eventId. Ensures partition affinity and decouples ingestion from heavy business execution.',
      tag: 'At-Least-Once Delivery',
      icon: Layers,
      badgeColor: 'border-amber-500/30 text-amber-400 bg-amber-500/10',
    },
    {
      id: 3,
      title: 'Redis Fast-Path Cache',
      subtitle: 'Key: event-processing:{id}',
      desc: 'Consumer checks Redis. If event status is COMPLETED, consumer acknowledges immediately (< 5ms) without hitting PostgreSQL.',
      tag: 'Non-Authoritative Cache',
      icon: Zap,
      badgeColor: 'border-emerald-500/30 text-emerald-400 bg-emerald-500/10',
    },
    {
      id: 4,
      title: 'Fencing Token & Lease',
      subtitle: 'PostgreSQL: processed_events',
      desc: 'Consumer acquires an atomic lease with a unique UUID fencing token (REQUIRES_NEW). Defends against zombie workers and concurrent duplicate deliveries.',
      tag: 'Authoritative Arbiter',
      icon: Shield,
      badgeColor: 'border-indigo-500/30 text-indigo-400 bg-indigo-500/10',
    },
    {
      id: 5,
      title: 'Atomic Business Commit',
      subtitle: 'payments / orders',
      desc: 'Business logic executes and persists the business entity inside an atomic database transaction. Event marked COMPLETED in PostgreSQL & cached in Redis.',
      tag: 'Exactly-Once Business Effect',
      icon: Database,
      badgeColor: 'border-emerald-500/30 text-emerald-400 bg-emerald-500/10',
    },
  ];

  return (
    <div className="glass-panel rounded-2xl sm:rounded-3xl p-5 sm:p-7 shadow-xl relative overflow-hidden">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-4 sm:pb-6 border-b border-slate-800/80 gap-3">
        <div>
          <h2 className="text-sm sm:text-base font-bold text-white flex items-center gap-2">
            <Radio className="w-4 h-4 sm:w-5 sm:h-5 text-indigo-400 animate-pulse shrink-0" />
            <span>Distributed Idempotency Pipeline Architecture</span>
          </h2>
          <p className="text-[11px] sm:text-xs text-slate-400 mt-0.5">
            Hover or click on any pipeline node to inspect how each distributed system guarantee is enforced.
          </p>
        </div>
        <div className="flex items-center gap-2 text-[11px] sm:text-xs font-mono bg-slate-950/80 px-3 py-1.5 rounded-xl border border-slate-800 text-slate-300 self-start sm:self-auto shadow-inner">
          <span className="w-2 h-2 rounded-full bg-emerald-400 animate-ping shadow-[0_0_8px_rgba(52,211,153,1)]" />
          <span>Active Pipeline: KRaft Mode</span>
        </div>
      </div>

      {/* Visual Pipeline Nodes */}
      <div className="mt-6 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3.5 relative">
        {steps.map((step, idx) => {
          const Icon = step.icon;
          const isSelected = activeStep === step.id;

          return (
            <motion.div 
              key={step.id} 
              whileHover={{ y: -4, scale: 1.01 }}
              onClick={() => setActiveStep(step.id)}
              className={`w-full p-4 rounded-xl border transition-all cursor-pointer flex flex-col justify-between min-h-[170px] relative group ${
                isSelected
                  ? 'glass-panel-glow text-white shadow-lg'
                  : 'glass-panel hover:border-indigo-500/40'
              }`}
            >
              <div>
                <div className="flex items-center justify-between mb-3">
                  <span className="text-xs font-mono font-bold text-slate-500">0{step.id}</span>
                  <span className={`text-[10px] font-semibold px-2 py-0.5 rounded-full border ${step.badgeColor}`}>
                    {step.tag}
                  </span>
                </div>
                <div className="flex items-center gap-2 mb-1">
                  <Icon className="w-4 h-4 text-indigo-400 shrink-0" />
                  <h3 className="text-sm font-bold text-white leading-tight">{step.title}</h3>
                </div>
                <p className="text-xs font-mono text-cyan-300/80">{step.subtitle}</p>
              </div>

              <div className="pt-2 text-[11px] text-slate-400 line-clamp-2">{step.desc}</div>
            </motion.div>
          );
        })}
      </div>

      {/* Detailed Step Inspector Box */}
      <div className="mt-8 bg-slate-950/80 border border-slate-800/80 rounded-2xl p-5 shadow-inner">
        <div className="flex items-start gap-3.5">
          <HelpCircle className="w-5 h-5 text-indigo-400 shrink-0 mt-0.5" />
          <div>
            <h4 className="text-sm font-bold text-white">
              {activeStep ? steps.find(s => s.id === activeStep)?.title : 'Understanding The Guarantees'}
            </h4>
            <p className="text-xs text-slate-400 mt-1 leading-relaxed">
              {activeStep
                ? steps.find(s => s.id === activeStep)?.desc
                : 'Distributed message brokers like Apache Kafka guarantee At-Least-Once delivery. This system achieves Exactly-Once business semantics through PostgreSQL transactional atomic token assertions (`WHERE processing_token = :token`), backed by a non-authoritative Redis cache for instant sub-millisecond duplicate short-circuiting.'}
            </p>
          </div>
        </div>

        {/* DLQ Resilience Footer Box */}
        <div className="mt-4 pt-4 border-t border-slate-900 flex flex-col sm:flex-row items-center justify-between gap-3 text-xs">
          <div className="flex items-center gap-2 text-rose-400">
            <AlertOctagon className="w-4 h-4 shrink-0" />
            <span>Dead Letter Queue (DLQ): Poison pills route to <code>events.dlq</code> after 3 exponential backoff attempts.</span>
          </div>
          <div className="flex items-center gap-2 text-emerald-400">
            <CheckCircle2 className="w-4 h-4 shrink-0" />
            <span>Fencing Protection: Zero rows updated if worker lease expires.</span>
          </div>
        </div>
      </div>
    </div>
  );
};
