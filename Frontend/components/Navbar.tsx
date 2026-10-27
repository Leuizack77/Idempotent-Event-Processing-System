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
  Radio,
  Sparkles
} from 'lucide-react';
import { motion } from 'motion/react';
import { ClusterStatus } from '@/lib/types';

interface NavbarProps {
  activeTab: string;
  setActiveTab: (tab: string) => void;
  cluster: ClusterStatus;
  onRefresh: () => void;
  isTourOpen?: boolean;
  onToggleTour?: () => void;
}

export const Navbar: React.FC<NavbarProps> = ({ 
  activeTab, 
  setActiveTab, 
  cluster, 
  onRefresh,
  isTourOpen,
  onToggleTour
}) => {
  const allHealthy = cluster.app === 'UP' && cluster.kafka === 'UP' && cluster.postgres === 'UP' && cluster.redis === 'UP';

  const navItems = [
    { id: '3d', label: '3D Topology', icon: Box },
    { id: 'simulator', label: 'Chaos & Ingestion', icon: Flame },
    { id: 'proof', label: 'Side-by-Side Proof', icon: Split },
    { id: 'traces', label: 'Trace Waterfall', icon: Clock },
    { id: 'pipeline', label: 'Pipeline', icon: Activity },
    { id: 'ledger', label: 'Event Ledger', icon: Layers },
    { id: 'business', label: 'Database', icon: Database },
  ];

  return (
    <header className="border-b border-zinc-800/80 bg-zinc-950/85 backdrop-blur-xl sticky top-0 z-50 transition-all">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-2.5 sm:py-3">
        {/* Top row: Brand + Cluster Health + Actions */}
        <div className="flex items-center justify-between gap-3">
          {/* Brand */}
          <div className="flex items-center gap-3 shrink-0">
            <div className="h-9 w-9 rounded-xl bg-zinc-900 border border-zinc-800 flex items-center justify-center shadow-sm">
              <ShieldCheck className="w-5 h-5 text-emerald-400" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <span className="font-bold text-white text-base tracking-tight">
                  FluxGuard
                </span>
                <span className="hidden sm:inline-flex px-2 py-0.5 text-[10px] font-medium bg-zinc-900 text-zinc-300 border border-zinc-800 rounded-md font-mono items-center gap-1">
                  v0.1.0 • Enterprise
                </span>
              </div>
              <p className="text-[11px] text-zinc-400 font-sans hidden xs:block">
                Distributed Idempotent Event Processing Engine
              </p>
            </div>
          </div>

          {/* Cluster Status & Actions */}
          <div className="flex items-center gap-2.5 shrink-0">
            {/* Desktop Cluster Health Pills */}
            <div className="hidden lg:flex items-center gap-2.5 bg-zinc-900/60 px-3 py-1 rounded-lg border border-zinc-800/80 text-xs shadow-inner">
              <div className="flex items-center gap-1.5">
                <span className={`w-1.5 h-1.5 rounded-full ${cluster.app === 'UP' ? 'bg-emerald-400' : 'bg-zinc-600'}`} />
                <span className="text-zinc-300 font-mono text-[11px]">App</span>
              </div>
              <span className="text-zinc-700">•</span>
              <div className="flex items-center gap-1.5">
                <span className={`w-1.5 h-1.5 rounded-full ${cluster.kafka === 'UP' ? 'bg-emerald-400' : 'bg-zinc-600'}`} />
                <span className="text-zinc-300 font-mono text-[11px]">Kafka</span>
              </div>
              <span className="text-zinc-700">•</span>
              <div className="flex items-center gap-1.5">
                <span className={`w-1.5 h-1.5 rounded-full ${cluster.postgres === 'UP' ? 'bg-emerald-400' : 'bg-zinc-600'}`} />
                <span className="text-zinc-300 font-mono text-[11px]">Postgres</span>
              </div>
              <span className="text-zinc-700">•</span>
              <div className="flex items-center gap-1.5">
                <span className={`w-1.5 h-1.5 rounded-full ${cluster.redis === 'UP' ? 'bg-emerald-400' : 'bg-rose-500'}`} />
                <span className="text-zinc-300 font-mono text-[11px]">Redis</span>
              </div>
            </div>

            {/* Mobile Compact Status */}
            <div className="lg:hidden flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-zinc-900 border border-zinc-800 text-[11px] font-mono">
              <span className={`w-1.5 h-1.5 rounded-full ${allHealthy ? 'bg-emerald-400' : 'bg-amber-400'}`} />
              <span className="text-zinc-300">{allHealthy ? 'Cluster UP' : 'Standby'}</span>
            </div>

            {/* Guided Tour Mission Button */}
            {onToggleTour && (
              <button
                onClick={onToggleTour}
                className={`px-2.5 py-1 sm:py-1.5 rounded-lg text-xs font-medium flex items-center gap-1.5 transition-all border ${
                  isTourOpen
                    ? 'bg-zinc-800 border-zinc-700 text-white shadow-sm'
                    : 'bg-zinc-900 hover:bg-zinc-850 border-zinc-800 text-zinc-400 hover:text-zinc-200'
                }`}
              >
                <Sparkles className="w-3.5 h-3.5 text-zinc-300" />
                <span className="hidden xs:inline">Guided Tour</span>
                <span className="xs:hidden">Tour</span>
              </button>
            )}

            {/* Refresh Button */}
            <button
              onClick={onRefresh}
              title="Refresh Cluster Status"
              className="p-1 sm:p-1.5 rounded-lg bg-zinc-900 hover:bg-zinc-850 text-zinc-400 hover:text-zinc-200 border border-zinc-800 transition"
            >
              <RefreshCw className="w-3.5 h-3.5" />
            </button>
          </div>
        </div>

        {/* Linear-Style Segmented Navigation Tab Bar */}
        <div className="mt-2.5 pt-2 border-t border-zinc-900 overflow-x-auto no-scrollbar py-0.5">
          <nav className="flex items-center gap-1 min-w-max bg-zinc-900/40 p-1 rounded-xl border border-zinc-800/60">
            {navItems.map((item) => {
              const Icon = item.icon;
              const isActive = activeTab === item.id;
              return (
                <button
                  key={item.id}
                  onClick={() => setActiveTab(item.id)}
                  className={`relative px-3 py-1.5 rounded-lg text-xs sm:text-sm font-medium transition-colors flex items-center gap-1.5 shrink-0 z-10 ${
                    isActive 
                      ? 'text-white' 
                      : 'text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800/40'
                  }`}
                >
                  {isActive && (
                    <motion.div
                      layoutId="activeNavTab"
                      className="absolute inset-0 bg-zinc-800 rounded-lg border border-zinc-700/60 shadow-sm -z-10"
                      transition={{ type: 'spring', bounce: 0.15, duration: 0.35 }}
                    />
                  )}
                  <Icon className={`w-3.5 h-3.5 ${isActive ? 'text-zinc-100' : 'text-zinc-400'}`} />
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
