'use client';

import React, { useState, useEffect, useCallback } from 'react';
import { Navbar } from '@/components/Navbar';
import { MetricsCards } from '@/components/MetricsCards';
import { PipelineVisualizer } from '@/components/PipelineVisualizer';
import { EventSimulator } from '@/components/EventSimulator';
import { ChaosStudio } from '@/components/ChaosStudio';
import { SplitScreenProof } from '@/components/SplitScreenProof';
import { TraceWaterfall } from '@/components/TraceWaterfall';
import { EventLedgerTable } from '@/components/EventLedgerTable';
import { BusinessRecordsViewer } from '@/components/BusinessRecordsViewer';
import { Cluster3DCanvas } from '@/components/Cluster3DCanvas';
import { Metrics3DChart } from '@/components/Metrics3DChart';
import { GuidedTourHUD } from '@/components/GuidedTourHUD';
import { eventService } from '@/lib/api';
import { ClusterStatus, OrderRecord, PaymentRecord, ProcessedEventRecord, SystemMetrics } from '@/lib/types';
import { RotateCcw, ShieldCheck, Box, Sparkles, Terminal } from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';

export default function Home() {
  const [activeTab, setActiveTab] = useState<string>('3d');
  const [isTourOpen, setIsTourOpen] = useState<boolean>(true);
  const [cluster, setCluster] = useState<ClusterStatus>({
    app: 'DOWN',
    postgres: 'DOWN',
    kafka: 'DOWN',
    redis: 'DOWN',
  });

  const [metrics, setMetrics] = useState<SystemMetrics>(eventService.getMetrics());
  const [events, setEvents] = useState<ProcessedEventRecord[]>(eventService.getEvents());
  const [payments, setPayments] = useState<PaymentRecord[]>(eventService.getPayments());
  const [orders, setOrders] = useState<OrderRecord[]>(eventService.getOrders());

  const refreshAll = useCallback(async () => {
    const clusterStatus = await eventService.checkClusterHealth();
    setCluster(clusterStatus);
    setMetrics(eventService.getMetrics());
    setEvents(eventService.getEvents());
    setPayments(eventService.getPayments());
    setOrders(eventService.getOrders());
  }, []);

  useEffect(() => {
    refreshAll();
    const interval = setInterval(refreshAll, 10000); // 10s health check polling
    return () => clearInterval(interval);
  }, [refreshAll]);

  const handleReset = () => {
    if (confirm('Reset demonstration state back to default?')) {
      eventService.resetAll();
      refreshAll();
    }
  };

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-100 flex flex-col font-sans selection:bg-zinc-800 selection:text-white relative overflow-x-hidden">
      {/* Linear / Vercel Ultra-Clean Dark Backdrop */}
      <div className="fixed inset-0 pointer-events-none overflow-hidden -z-10">
        {/* Top subtle vignette glow */}
        <div className="absolute top-0 left-1/2 -translate-x-1/2 w-full max-w-7xl h-[420px] bg-[radial-gradient(ellipse_at_top,rgba(99,102,241,0.07),transparent_70%)]" />
        {/* Subtle architectural micro-grid */}
        <div className="absolute inset-0 bg-[linear-gradient(to_right,#ffffff05_1px,transparent_1px),linear-gradient(to_bottom,#ffffff05_1px,transparent_1px)] bg-[size:4rem_4rem] [mask-image:radial-gradient(ellipse_60%_50%_at_50%_0%,#000_70%,transparent_100%)]" />
      </div>

      {/* Top Navbar */}
      <Navbar
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        cluster={cluster}
        onRefresh={refreshAll}
        isTourOpen={isTourOpen}
        onToggleTour={() => setIsTourOpen((prev) => !prev)}
      />

      {/* Main Content Area */}
      <main className="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-6 space-y-6">
        {/* Top Header Hero */}
        <motion.div 
          initial={{ opacity: 0, y: -6 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.3 }}
          className="flex flex-col md:flex-row md:items-center justify-between pb-5 border-b border-zinc-800/80 gap-4"
        >
          <div>
            <div className="flex items-center gap-2.5 flex-wrap">
              <h1 className="text-xl sm:text-2xl font-bold tracking-tight text-white flex items-center gap-2">
                <span>FluxGuard</span>
                <span className="text-zinc-600 font-normal">/</span>
                <span className="text-zinc-300 font-medium text-base sm:text-lg">
                  Distributed Idempotency Engine
                </span>
              </h1>
              <span className="inline-flex px-2.5 py-0.5 rounded-full text-xs font-mono font-medium bg-zinc-900 text-emerald-400 border border-emerald-500/20 items-center gap-1.5 shadow-sm">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 inline-block" />
                <ShieldCheck className="w-3.5 h-3.5 text-emerald-400" /> Exactly-Once Guarantee
              </span>
            </div>
            <p className="text-xs sm:text-sm text-zinc-400 mt-1 max-w-3xl leading-relaxed">
              Fault-tolerant distributed stream cockpit demonstrating <strong>At-Least-Once Kafka Delivery with Exactly-Once Business Effect</strong> via atomic token fencing and Redis L1 fast-path caching.
            </p>
          </div>

          <div className="flex items-center gap-2 shrink-0">
            <button
              onClick={() => setActiveTab('3d')}
              className={`text-xs px-3 py-1.5 rounded-lg border flex items-center gap-1.5 font-medium transition-all ${
                activeTab === '3d'
                  ? 'bg-zinc-800 border-zinc-700 text-white shadow-sm'
                  : 'bg-zinc-900 hover:bg-zinc-850 border-zinc-800/80 text-zinc-400 hover:text-zinc-200'
              }`}
            >
              <Box className="w-3.5 h-3.5 text-zinc-300" />
              3D View
            </button>

            <button
              onClick={handleReset}
              className="text-xs px-3 py-1.5 bg-zinc-900 hover:bg-zinc-850 text-zinc-400 hover:text-zinc-200 border border-zinc-800/80 rounded-lg flex items-center gap-1.5 font-medium transition-all"
            >
              <RotateCcw className="w-3.5 h-3.5 text-zinc-500" />
              Reset
            </button>
          </div>
        </motion.div>

        {/* Global Real-time Metrics Cards (Motion Staggered & Glow) */}
        <MetricsCards metrics={metrics} />

        {/* 21st.dev Fluid Tab Transitions via AnimatePresence */}
        <AnimatePresence mode="wait">
          <motion.div
            key={activeTab}
            initial={{ opacity: 0, y: 14, filter: 'blur(4px)' }}
            animate={{ opacity: 1, y: 0, filter: 'blur(0px)' }}
            exit={{ opacity: 0, y: -10, filter: 'blur(4px)' }}
            transition={{ duration: 0.28, ease: [0.16, 1, 0.3, 1] }}
            className="space-y-8"
          >
            {/* Tab 0: 3D Topology & Holographic Visualizer */}
            {activeTab === '3d' && (
              <div className="space-y-8">
                <Cluster3DCanvas metrics={metrics} onTriggerBurst={refreshAll} />
                <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
                  <div className="lg:col-span-5">
                    <Metrics3DChart metrics={metrics} />
                  </div>
                  <div className="lg:col-span-7">
                    <ChaosStudio onScenarioRun={refreshAll} />
                  </div>
                </div>
              </div>
            )}

            {/* Tab 1: Chaos & Ingestion Studio */}
            {activeTab === 'simulator' && (
              <div className="space-y-8">
                <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
                  <div className="lg:col-span-8">
                    <ChaosStudio onScenarioRun={refreshAll} />
                  </div>
                  <div className="lg:col-span-4">
                    <Metrics3DChart metrics={metrics} />
                  </div>
                </div>
                <EventSimulator onEventSubmitted={refreshAll} />
                <SplitScreenProof events={events} payments={payments} orders={orders} onRefresh={refreshAll} />
              </div>
            )}

            {/* Tab 2: Side-by-Side Proof */}
            {activeTab === 'proof' && (
              <div className="space-y-8">
                <SplitScreenProof events={events} payments={payments} orders={orders} onRefresh={refreshAll} />
                <BusinessRecordsViewer payments={payments} orders={orders} />
              </div>
            )}

            {/* Tab 3: Trace Waterfall */}
            {activeTab === 'traces' && (
              <div className="space-y-8">
                <TraceWaterfall events={events} />
              </div>
            )}

            {/* Tab 4: Pipeline Visualizer */}
            {activeTab === 'pipeline' && (
              <div className="space-y-8">
                <Cluster3DCanvas metrics={metrics} onTriggerBurst={refreshAll} />
                <PipelineVisualizer />
              </div>
            )}

            {/* Tab 5: Event Ledger */}
            {activeTab === 'ledger' && (
              <div className="space-y-8">
                <EventLedgerTable events={events} />
              </div>
            )}

            {/* Tab 6: Business Database Proof */}
            {activeTab === 'business' && (
              <div className="space-y-8">
                <BusinessRecordsViewer payments={payments} orders={orders} />
              </div>
            )}
          </motion.div>
        </AnimatePresence>
      </main>

      {/* Linear-Style Minimalist Footer */}
      <footer className="border-t border-zinc-800/80 bg-zinc-950/80 backdrop-blur-xl py-5 px-6 text-center text-xs text-zinc-500 mt-12">
        <div className="max-w-7xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <span className="font-semibold text-zinc-300">FluxGuard</span>
            <span>•</span>
            <span>Distributed Idempotency Engine & Observability</span>
          </div>
          <div className="flex items-center gap-4 text-zinc-400 font-mono text-[11px]">
            <span className="flex items-center gap-1 text-zinc-300">
              <Box className="w-3 h-3 text-zinc-400" /> Three.js WebGL
            </span>
            <span>•</span>
            <span>Kafka KRaft</span>
            <span>•</span>
            <span>Postgres ACID</span>
            <span>•</span>
            <span className="text-emerald-400">Redis L1 Cache</span>
          </div>
        </div>
      </footer>

      {/* Interactive Guided Mission HUD */}
      <GuidedTourHUD
        isOpen={isTourOpen}
        onClose={() => setIsTourOpen(false)}
        cluster={cluster}
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        onRefreshAll={refreshAll}
      />
    </div>
  );
}
