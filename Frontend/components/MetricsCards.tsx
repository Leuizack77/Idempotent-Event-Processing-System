'use client';

import React from 'react';
import { CheckCircle2, ShieldAlert, Zap, Clock, AlertTriangle, Layers } from 'lucide-react';
import { motion } from 'motion/react';
import { SystemMetrics } from '@/lib/types';
import { SpotlightCard } from '@/components/ui/SpotlightCard';

interface MetricsCardsProps {
  metrics: SystemMetrics;
}

export const MetricsCards: React.FC<MetricsCardsProps> = ({ metrics }) => {
  const container = {
    hidden: { opacity: 0 },
    show: {
      opacity: 1,
      transition: {
        staggerChildren: 0.06,
      },
    },
  };

  const item = {
    hidden: { opacity: 0, y: 10 },
    show: { opacity: 1, y: 0, transition: { duration: 0.25, ease: 'easeOut' as const } },
  };

  return (
    <motion.div 
      variants={container}
      initial="hidden"
      animate="show"
      className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3.5"
    >
      {/* 1. Processed Events */}
      <motion.div variants={item} whileHover={{ y: -2, transition: { duration: 0.15 } }}>
        <SpotlightCard className="p-4 sm:p-5 h-full">
          <div className="flex items-center justify-between">
            <span className="text-xs font-medium text-zinc-400 flex items-center gap-1.5">
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
              Processed Events
            </span>
            <div className="p-1.5 rounded-lg bg-zinc-800/80 text-emerald-400 border border-zinc-700/60">
              <CheckCircle2 className="w-4 h-4" />
            </div>
          </div>
          <div className="mt-3 flex items-baseline gap-2">
            <span className="text-2xl sm:text-3xl font-bold text-white font-mono tracking-tight">{metrics.totalProcessed}</span>
            <span className="text-xs text-zinc-500 font-mono">/ {metrics.totalSubmitted} ingested</span>
          </div>
          <div className="mt-2.5 pt-2 border-t border-zinc-800/80 flex items-center justify-between text-xs">
            <span className="text-emerald-400 font-medium text-[11px] font-mono flex items-center gap-1">
              100% Exactly-Once
            </span>
            <span className="text-zinc-500 font-mono text-[10px]">Authoritative Ledger</span>
          </div>
        </SpotlightCard>
      </motion.div>

      {/* 2. Duplicates Intercepted */}
      <motion.div variants={item} whileHover={{ y: -2, transition: { duration: 0.15 } }}>
        <SpotlightCard className="p-4 sm:p-5 h-full">
          <div className="flex items-center justify-between">
            <span className="text-xs font-medium text-zinc-400 flex items-center gap-1.5">
              <span className="w-1.5 h-1.5 rounded-full bg-indigo-400" />
              Duplicates Defended
            </span>
            <div className="p-1.5 rounded-lg bg-zinc-800/80 text-indigo-400 border border-zinc-700/60">
              <ShieldAlert className="w-4 h-4" />
            </div>
          </div>
          <div className="mt-3 flex items-baseline gap-2">
            <span className="text-2xl sm:text-3xl font-bold text-white font-mono tracking-tight">{metrics.totalDuplicatesBlocked}</span>
            <span className="text-xs text-zinc-500 font-mono">intercepted</span>
          </div>
          <div className="mt-2.5 pt-2 border-t border-zinc-800/80 flex items-center justify-between text-xs font-mono text-[11px]">
            <span className="text-zinc-300 flex items-center gap-1">
              <Zap className="w-3 h-3 text-emerald-400" /> Redis: {metrics.redisCacheHits}
            </span>
            <span className="text-zinc-700">•</span>
            <span className="text-zinc-300 flex items-center gap-1">
              <Layers className="w-3 h-3 text-indigo-400" /> DB: {metrics.postgresConstraintHits}
            </span>
          </div>
        </SpotlightCard>
      </motion.div>

      {/* 3. Dead Letter Queue */}
      <motion.div variants={item} whileHover={{ y: -2, transition: { duration: 0.15 } }}>
        <SpotlightCard className="p-4 sm:p-5 h-full">
          <div className="flex items-center justify-between">
            <span className="text-xs font-medium text-zinc-400 flex items-center gap-1.5">
              <span className="w-1.5 h-1.5 rounded-full bg-rose-400" />
              Dead Letter Queue
            </span>
            <div className="p-1.5 rounded-lg bg-zinc-800/80 text-rose-400 border border-zinc-700/60">
              <AlertTriangle className="w-4 h-4" />
            </div>
          </div>
          <div className="mt-3 flex items-baseline gap-2">
            <span className="text-2xl sm:text-3xl font-bold text-white font-mono tracking-tight">{metrics.dlqTotal}</span>
            <span className="text-xs text-zinc-500 font-mono">isolated</span>
          </div>
          <div className="mt-2.5 pt-2 border-t border-zinc-800/80 flex items-center justify-between text-xs font-mono text-[11px]">
            <span className="text-rose-400">events.dlq topic</span>
            <span className="text-zinc-500 text-[10px]">Poison Pills</span>
          </div>
        </SpotlightCard>
      </motion.div>

      {/* 4. Processing Latency */}
      <motion.div variants={item} whileHover={{ y: -2, transition: { duration: 0.15 } }}>
        <SpotlightCard className="p-4 sm:p-5 h-full">
          <div className="flex items-center justify-between">
            <span className="text-xs font-medium text-zinc-400 flex items-center gap-1.5">
              <span className="w-1.5 h-1.5 rounded-full bg-amber-400" />
              Avg Latency
            </span>
            <div className="p-1.5 rounded-lg bg-zinc-800/80 text-amber-400 border border-zinc-700/60">
              <Clock className="w-4 h-4" />
            </div>
          </div>
          <div className="mt-3 flex items-baseline gap-2">
            <span className="text-2xl sm:text-3xl font-bold text-white font-mono tracking-tight">{metrics.averageLatencyMs}</span>
            <span className="text-xs text-zinc-500 font-mono">ms execution</span>
          </div>
          <div className="mt-2.5 pt-2 border-t border-zinc-800/80 flex items-center justify-between text-xs font-mono text-[11px]">
            <span className="text-emerald-400">L1 Fast: &lt;3ms</span>
            <span className="text-zinc-700">•</span>
            <span className="text-zinc-400">L2 DB: ~42ms</span>
          </div>
        </SpotlightCard>
      </motion.div>
    </motion.div>
  );
};
