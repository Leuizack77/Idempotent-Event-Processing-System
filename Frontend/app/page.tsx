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
import { eventService } from '@/lib/api';
import { ClusterStatus, OrderRecord, PaymentRecord, ProcessedEventRecord, SystemMetrics } from '@/lib/types';
import { RotateCcw, ShieldCheck, Box, Sparkles } from 'lucide-react';

export default function Home() {
  const [activeTab, setActiveTab] = useState<string>('3d');
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
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans selection:bg-indigo-500 selection:text-white">
      {/* Top Navbar */}
      <Navbar
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        cluster={cluster}
        onRefresh={refreshAll}
      />

      {/* Main Content Area */}
      <main className="flex-1 max-w-7xl w-full mx-auto px-3 sm:px-6 lg:px-8 py-4 sm:py-6 space-y-6 sm:space-y-8">
        {/* Top Header Hero */}
        <div className="flex flex-col md:flex-row md:items-center justify-between pb-3 sm:pb-4 border-b border-slate-900 gap-3 sm:gap-4">
          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-xl sm:text-2xl md:text-3xl font-extrabold text-white tracking-tight">
                Idempotent Event Processing
              </h1>
              <span className="hidden xs:inline-flex px-2 sm:px-2.5 py-0.5 rounded-full text-[10px] sm:text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 items-center gap-1 font-mono">
                <ShieldCheck className="w-3 h-3 sm:w-3.5 sm:h-3.5" /> High-Throughput
              </span>
            </div>
            <p className="text-xs sm:text-sm text-slate-400 mt-1 max-w-3xl leading-relaxed">
              Production distributed systems cockpit demonstrating <strong>At-Least-Once Delivery with Exactly-Once Business Effect</strong> across Apache Kafka, PostgreSQL atomic token fencing, and Redis L1 fast-path caching.
            </p>
          </div>

          <div className="flex items-center gap-2">
            <button
              onClick={() => setActiveTab('3d')}
              className={`text-xs px-3.5 py-2 rounded-xl border flex items-center gap-1.5 transition ${
                activeTab === '3d'
                  ? 'bg-indigo-600 border-indigo-500 text-white'
                  : 'bg-slate-900 border-slate-800 text-slate-400 hover:text-white'
              }`}
            >
              <Box className="w-3.5 h-3.5 text-cyan-400" />
              3D View
            </button>

            <button
              onClick={handleReset}
              className="text-xs px-3.5 py-2 bg-slate-900 hover:bg-slate-800 text-slate-400 hover:text-slate-200 border border-slate-800 rounded-xl flex items-center gap-1.5 transition"
            >
              <RotateCcw className="w-3.5 h-3.5" />
              Reset State
            </button>
          </div>
        </div>

        {/* Global Real-time Metrics Cards */}
        <MetricsCards metrics={metrics} />

        {/* Tab 0: 3D Topology & Holographic Visualizer */}
        {activeTab === '3d' && (
          <div className="space-y-8 animate-fadeIn">
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
      </main>

      {/* Footer */}
      <footer className="border-t border-slate-900 bg-slate-950 py-6 px-6 text-center text-xs text-slate-500">
        <div className="max-w-7xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-3">
          <span>Idempotent Event Processing Architecture • Spring Boot 3.3.4 & Next.js 16</span>
          <div className="flex items-center gap-4 text-slate-400 font-mono text-[11px]">
            <span>Three.js WebGL</span>
            <span>•</span>
            <span>Kafka KRaft</span>
            <span>•</span>
            <span>Postgres ACID</span>
            <span>•</span>
            <span>Redis L1 Cache</span>
          </div>
        </div>
      </footer>
    </div>
  );
}
