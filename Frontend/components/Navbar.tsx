'use client';

import React from 'react';
import { 
  Activity, 
  ShieldCheck, 
  Database, 
  RefreshCw, 
  Flame, 
  Clock, 
  Split, 
  Layers,
  Box,
  CheckCircle2
} from 'lucide-react';
import { ClusterStatus } from '@/lib/types';

interface NavbarProps {
  activeTab: string;
  setActiveTab: (tab: string) => void;
  cluster: ClusterStatus;
  onRefresh: () => void;
}

export const Navbar: React.FC<NavbarProps> = ({ activeTab, setActiveTab, cluster, onRefresh }) => {
  const allHealthy = cluster.app === 'UP' && cluster.kafka === 'UP' && cluster.postgres === 'UP' && cluster.redis === 'UP';

  const navItems = [
    { id: '3d', label: '3D Topology', icon: Box, iconColor: 'text-cyan-400' },
    { id: 'simulator', label: 'Chaos & Ingestion', icon: Flame, iconColor: 'text-amber-400' },
    { id: 'proof', label: 'Side-by-Side Proof', icon: Split, iconColor: 'text-emerald-400' },
    { id: 'traces', label: 'Trace Waterfall', icon: Clock, iconColor: 'text-indigo-400' },
    { id: 'pipeline', label: 'Pipeline', icon: Activity, iconColor: 'text-purple-400' },
    { id: 'ledger', label: 'Event Ledger', icon: Layers, iconColor: 'text-blue-400' },
    { id: 'business', label: 'Database', icon: Database, iconColor: 'text-slate-300' },
  ];

  return (
    <header className="border-b border-slate-800/80 bg-slate-950/90 backdrop-blur-md sticky top-0 z-50 transition-all">
      <div className="max-w-7xl mx-auto px-3 sm:px-6 py-2.5 sm:py-3.5">
        {/* Top row: Brand + Status + Refresh */}
        <div className="flex items-center justify-between gap-3">
          {/* Brand */}
          <div className="flex items-center gap-2.5 sm:gap-3 shrink-0">
            <div className="h-9 w-9 sm:h-10 sm:w-10 rounded-xl bg-gradient-to-tr from-indigo-600 via-indigo-500 to-emerald-400 flex items-center justify-center shadow-lg shadow-indigo-500/20 shrink-0">
              <ShieldCheck className="w-5 h-5 sm:w-6 sm:h-6 text-white" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <span className="font-extrabold text-white text-base sm:text-lg tracking-tight">
                  Idempotency Cockpit
                </span>
                <span className="hidden sm:inline-flex px-2 py-0.5 text-[10px] sm:text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 rounded-full font-mono">
                  Exactly-Once
                </span>
              </div>
              <p className="text-[11px] sm:text-xs text-slate-400 font-sans hidden xs:block">
                Kafka KRaft • Redis Cache • Postgres ACID
              </p>
            </div>
          </div>

          {/* Desktop & Mobile Cluster Indicators */}
          <div className="flex items-center gap-2 sm:gap-3 shrink-0">
            {/* Desktop Cluster Pills */}
            <div className="hidden lg:flex items-center gap-2 bg-slate-900/90 px-3 py-1.5 rounded-xl border border-slate-800 text-xs">
              <div className="flex items-center gap-1.5">
                <span className={`w-2 h-2 rounded-full ${cluster.app === 'UP' ? 'bg-emerald-400' : 'bg-slate-500'}`} />
                <span className="text-slate-300 font-mono">App</span>
              </div>
              <span className="text-slate-700">•</span>
              <div className="flex items-center gap-1.5">
                <span className={`w-2 h-2 rounded-full ${cluster.kafka === 'UP' ? 'bg-emerald-400' : 'bg-slate-500'}`} />
                <span className="text-slate-300 font-mono">Kafka</span>
              </div>
              <span className="text-slate-700">•</span>
              <div className="flex items-center gap-1.5">
                <span className={`w-2 h-2 rounded-full ${cluster.postgres === 'UP' ? 'bg-emerald-400' : 'bg-slate-500'}`} />
                <span className="text-slate-300 font-mono">Postgres</span>
              </div>
              <span className="text-slate-700">•</span>
              <div className="flex items-center gap-1.5">
                <span className={`w-2 h-2 rounded-full ${cluster.redis === 'UP' ? 'bg-emerald-400' : 'bg-rose-500'}`} />
                <span className="text-slate-300 font-mono">Redis</span>
              </div>
            </div>

            {/* Mobile Compact Cluster Badge */}
            <div className="lg:hidden flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-slate-900 border border-slate-800 text-[11px] font-mono">
              <span className={`w-2 h-2 rounded-full ${allHealthy ? 'bg-emerald-400' : 'bg-amber-400 animate-pulse'}`} />
              <span className="text-slate-300">{allHealthy ? 'Live Cluster' : 'Standby'}</span>
            </div>

            {/* Refresh Button */}
            <button
              onClick={onRefresh}
              title="Refresh Cluster Status"
              className="p-1.5 sm:p-2 rounded-xl bg-slate-900 hover:bg-slate-800 text-slate-300 hover:text-white border border-slate-800 transition shadow-sm active:scale-95"
            >
              <RefreshCw className="w-3.5 h-3.5 sm:w-4 sm:h-4" />
            </button>
          </div>
        </div>

        {/* Bottom row / Horizontal Nav Bar (Smooth horizontal scroll on mobile, centered on desktop) */}
        <div className="mt-2.5 pt-2 border-t border-slate-900/60 overflow-x-auto no-scrollbar py-0.5">
          <nav className="flex items-center gap-1 min-w-max">
            {navItems.map((item) => {
              const Icon = item.icon;
              const isActive = activeTab === item.id;
              return (
                <button
                  key={item.id}
                  onClick={() => setActiveTab(item.id)}
                  className={`px-3 py-1.5 rounded-lg text-xs sm:text-sm font-medium transition-all flex items-center gap-1.5 shrink-0 ${
                    isActive
                      ? 'bg-indigo-600 text-white shadow-md shadow-indigo-600/30'
                      : 'text-slate-400 hover:text-white hover:bg-slate-900/90'
                  }`}
                >
                  <Icon className={`w-3.5 h-3.5 ${isActive ? 'text-white' : item.iconColor}`} />
                  <span>{item.label}</span>
                </button>
              );
            })}
          </nav>
        </div>
      </div>
    </header>
  );
};
