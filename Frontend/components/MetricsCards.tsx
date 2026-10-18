'use client';

import React from 'react';
import { CheckCircle2, ShieldAlert, Zap, Clock, AlertTriangle, Layers } from 'lucide-react';
import { SystemMetrics } from '@/lib/types';

interface MetricsCardsProps {
  metrics: SystemMetrics;
}

export const MetricsCards: React.FC<MetricsCardsProps> = ({ metrics }) => {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      {/* 1. Total Processed */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 sm:p-5 shadow-sm relative overflow-hidden group hover:border-slate-700 transition">
        <div className="flex items-center justify-between">
          <span className="text-[11px] sm:text-xs font-semibold text-slate-400 uppercase tracking-wider">Processed Events</span>
          <div className="p-1.5 sm:p-2 rounded-xl bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
            <CheckCircle2 className="w-4 h-4 sm:w-5 sm:h-5" />
          </div>
        </div>
        <div className="mt-3 sm:mt-4 flex items-baseline gap-2">
          <span className="text-2xl sm:text-3xl font-extrabold text-white font-mono">{metrics.totalProcessed}</span>
          <span className="text-[11px] sm:text-xs text-slate-400">/ {metrics.totalSubmitted} total</span>
        </div>
        <p className="mt-1 text-xs text-emerald-400/90 font-medium flex items-center gap-1">
          <span>Exactly-Once Committed</span>
        </p>
      </div>

      {/* 2. Duplicates Intercepted */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 sm:p-5 shadow-sm relative overflow-hidden group hover:border-slate-700 transition">
        <div className="flex items-center justify-between">
          <span className="text-[11px] sm:text-xs font-semibold text-slate-400 uppercase tracking-wider">Duplicates Blocked</span>
          <div className="p-1.5 sm:p-2 rounded-xl bg-indigo-500/10 text-indigo-400 border border-indigo-500/20">
            <ShieldAlert className="w-4 h-4 sm:w-5 sm:h-5" />
          </div>
        </div>
        <div className="mt-3 sm:mt-4 flex items-baseline gap-2">
          <span className="text-2xl sm:text-3xl font-extrabold text-indigo-400 font-mono">{metrics.totalDuplicatesBlocked}</span>
          <span className="text-[11px] sm:text-xs text-slate-400">deduped</span>
        </div>
        <div className="mt-2 flex items-center gap-3 text-xs font-mono">
          <span className="text-emerald-400 flex items-center gap-1">
            <Zap className="w-3 h-3" /> Redis: {metrics.redisCacheHits}
          </span>
          <span className="text-slate-600">|</span>
          <span className="text-indigo-300 flex items-center gap-1">
            <Layers className="w-3 h-3" /> DB: {metrics.postgresConstraintHits}
          </span>
        </div>
      </div>

      {/* 3. Dead Letter Queue */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 sm:p-5 shadow-sm relative overflow-hidden group hover:border-slate-700 transition">
        <div className="flex items-center justify-between">
          <span className="text-[11px] sm:text-xs font-semibold text-slate-400 uppercase tracking-wider">Dead Letter Queue</span>
          <div className="p-1.5 sm:p-2 rounded-xl bg-rose-500/10 text-rose-400 border border-rose-500/20">
            <AlertTriangle className="w-4 h-4 sm:w-5 sm:h-5" />
          </div>
        </div>
        <div className="mt-3 sm:mt-4 flex items-baseline gap-2">
          <span className="text-2xl sm:text-3xl font-extrabold text-rose-400 font-mono">{metrics.dlqTotal}</span>
          <span className="text-[11px] sm:text-xs text-slate-400">poison pills</span>
        </div>
        <p className="mt-1 text-xs text-rose-400/80 font-medium">
          events.dlq topic isolated
        </p>
      </div>

      {/* 4. Processing Latency */}
      <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 sm:p-5 shadow-sm relative overflow-hidden group hover:border-slate-700 transition">
        <div className="flex items-center justify-between">
          <span className="text-[11px] sm:text-xs font-semibold text-slate-400 uppercase tracking-wider">Avg Latency</span>
          <div className="p-1.5 sm:p-2 rounded-xl bg-amber-500/10 text-amber-400 border border-amber-500/20">
            <Clock className="w-4 h-4 sm:w-5 sm:h-5" />
          </div>
        </div>
        <div className="mt-3 sm:mt-4 flex items-baseline gap-2">
          <span className="text-2xl sm:text-3xl font-extrabold text-amber-400 font-mono">{metrics.averageLatencyMs}</span>
          <span className="text-[11px] sm:text-xs text-slate-400">ms avg</span>
        </div>
        <p className="mt-1 text-xs text-amber-400/80 font-medium">
          Fast-path &lt; 5ms / Full txn ~45ms
        </p>
      </div>
    </div>
  );
};
