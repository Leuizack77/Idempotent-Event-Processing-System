'use client';

import React, { useState } from 'react';
import { 
  Search, 
  Filter, 
  ChevronDown, 
  ChevronUp, 
  CheckCircle2, 
  AlertTriangle, 
  Clock, 
  ShieldCheck, 
  Key,
  Layers,
  Copy
} from 'lucide-react';
import { ProcessedEventRecord } from '@/lib/types';

interface EventLedgerTableProps {
  events: ProcessedEventRecord[];
}

export const EventLedgerTable: React.FC<EventLedgerTableProps> = ({ events }) => {
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [statusFilter, setStatusFilter] = useState<string>('ALL');
  const [expandedId, setExpandedId] = useState<string | null>(null);

  const filteredEvents = events.filter(evt => {
    const matchesSearch = 
      evt.eventId.toLowerCase().includes(searchTerm.toLowerCase()) ||
      evt.eventType.toLowerCase().includes(searchTerm.toLowerCase()) ||
      evt.processingToken.toLowerCase().includes(searchTerm.toLowerCase());
    
    const matchesStatus = statusFilter === 'ALL' || evt.status === statusFilter;

    return matchesSearch && matchesStatus;
  });

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'COMPLETED':
        return (
          <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
            <CheckCircle2 className="w-3 h-3" /> COMPLETED
          </span>
        );
      case 'FAILED':
        return (
          <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-rose-500/10 text-rose-400 border border-rose-500/20">
            <AlertTriangle className="w-3 h-3" /> FAILED (DLQ)
          </span>
        );
      case 'PROCESSING':
        return (
          <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-amber-500/10 text-amber-400 border border-amber-500/20">
            <Clock className="w-3 h-3 animate-spin" /> PROCESSING
          </span>
        );
      default:
        return (
          <span className="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold bg-blue-500/10 text-blue-400 border border-blue-500/20">
            {status}
          </span>
        );
    }
  };

  const getDeduplicationBadge = (source: string) => {
    switch (source) {
      case 'REDIS_FAST_PATH':
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[11px] font-mono bg-emerald-500/15 text-emerald-300 border border-emerald-500/30">
            ⚡ Redis Cache Hit
          </span>
        );
      case 'POSTGRES_CONSTRAINT':
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[11px] font-mono bg-indigo-500/15 text-indigo-300 border border-indigo-500/30">
            🛡️ Postgres Constraint
          </span>
        );
      default:
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-[11px] font-mono bg-slate-800 text-slate-400">
            First-time Execution
          </span>
        );
    }
  };

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm">
      {/* Header & Controls */}
      <div className="flex flex-col md:flex-row md:items-center justify-between pb-4 sm:pb-6 border-b border-slate-800 gap-4">
        <div>
          <h2 className="text-sm sm:text-base font-bold text-white flex items-center gap-2">
            <ShieldCheck className="w-4 h-4 sm:w-5 sm:h-5 text-indigo-400 shrink-0" />
            <span>Processed Events Ledger & Audit Trail</span>
          </h2>
          <p className="text-[11px] sm:text-xs text-slate-400 mt-0.5">
            Real-time audit log of all events received, tokens asserted, and deduplication sources.
          </p>
        </div>

        {/* Search & Filters */}
        <div className="flex flex-wrap items-center gap-2.5">
          <div className="relative flex-1 sm:flex-initial">
            <Search className="w-3.5 h-3.5 text-slate-500 absolute left-3 top-1/2 -translate-y-1/2" />
            <input
              type="text"
              placeholder="Search eventId, token..."
              value={searchTerm}
              onChange={e => setSearchTerm(e.target.value)}
              className="bg-slate-950 border border-slate-800 rounded-xl pl-9 pr-3.5 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 w-full sm:w-56"
            />
          </div>

          <div className="flex items-center gap-1 bg-slate-950 border border-slate-800 rounded-xl px-2.5 py-1 text-xs">
            <Filter className="w-3.5 h-3.5 text-slate-500 mr-1" />
            <select
              value={statusFilter}
              onChange={e => setStatusFilter(e.target.value)}
              className="bg-transparent text-slate-300 focus:outline-none cursor-pointer"
            >
              <option value="ALL">All Statuses</option>
              <option value="COMPLETED">COMPLETED</option>
              <option value="FAILED">FAILED (DLQ)</option>
              <option value="PROCESSING">PROCESSING</option>
            </select>
          </div>
        </div>
      </div>

      {/* Table with responsive horizontal scroll */}
      <div className="mt-4 overflow-x-auto no-scrollbar">
        <table className="w-full min-w-[680px] text-left text-xs">
          <thead>
            <tr className="border-b border-slate-800 text-slate-400 font-semibold uppercase tracking-wider">
              <th className="py-3 px-3">Event ID</th>
              <th className="py-3 px-3">Type</th>
              <th className="py-3 px-3">Status</th>
              <th className="py-3 px-3">Deduplication</th>
              <th className="py-3 px-3">Fencing Token</th>
              <th className="py-3 px-3 text-right">Latency</th>
              <th className="py-3 px-3 text-center">Action</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/60 font-mono">
            {filteredEvents.length === 0 ? (
              <tr>
                <td colSpan={7} className="text-center py-8 text-slate-500">
                  No event records found matching criteria.
                </td>
              </tr>
            ) : (
              filteredEvents.map(evt => {
                const isExpanded = expandedId === evt.id;
                return (
                  <React.Fragment key={evt.id}>
                    <tr className="hover:bg-slate-800/40 transition">
                      <td className="py-3 px-3 font-semibold text-white">
                        <div className="flex items-center gap-1.5">
                          <span>{evt.eventId}</span>
                          <button
                            onClick={() => navigator.clipboard.writeText(evt.eventId)}
                            className="text-slate-600 hover:text-slate-300"
                            title="Copy eventId"
                          >
                            <Copy className="w-3 h-3" />
                          </button>
                        </div>
                      </td>
                      <td className="py-3 px-3 text-slate-300 text-[11px] font-sans font-medium">
                        {evt.eventType}
                      </td>
                      <td className="py-3 px-3">{getStatusBadge(evt.status)}</td>
                      <td className="py-3 px-3">{getDeduplicationBadge(evt.deduplicationSource)}</td>
                      <td className="py-3 px-3 text-slate-400 text-[11px] truncate max-w-[140px]" title={evt.processingToken}>
                        <span className="flex items-center gap-1">
                          <Key className="w-3 h-3 text-indigo-400 shrink-0" />
                          {evt.processingToken.substring(0, 8)}...
                        </span>
                      </td>
                      <td className="py-3 px-3 text-right text-slate-300 font-bold">
                        {evt.durationMs}ms
                      </td>
                      <td className="py-3 px-3 text-center">
                        <button
                          onClick={() => setExpandedId(isExpanded ? null : evt.id)}
                          className="p-1 rounded hover:bg-slate-800 text-slate-400 hover:text-white transition"
                          title="Toggle details"
                        >
                          {isExpanded ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
                        </button>
                      </td>
                    </tr>

                    {/* Expandable Details Row */}
                    {isExpanded && (
                      <tr className="bg-slate-950/80 border-y border-slate-800">
                        <td colSpan={7} className="p-4">
                          <div className="space-y-3 font-sans text-xs">
                            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                              <div className="bg-slate-900 p-2.5 rounded-lg border border-slate-800">
                                <span className="text-[11px] text-slate-500 font-semibold block">Full Fencing Token</span>
                                <span className="font-mono text-indigo-300 text-[11px] break-all">{evt.processingToken}</span>
                              </div>
                              <div className="bg-slate-900 p-2.5 rounded-lg border border-slate-800">
                                <span className="text-[11px] text-slate-500 font-semibold block">Retry Count</span>
                                <span className="font-mono text-slate-200">{evt.retryCount} retries</span>
                              </div>
                              <div className="bg-slate-900 p-2.5 rounded-lg border border-slate-800">
                                <span className="text-[11px] text-slate-500 font-semibold block">Created Timestamp</span>
                                <span className="font-mono text-slate-200">{evt.createdAt}</span>
                              </div>
                            </div>

                            {evt.errorMessage && (
                              <div className="p-3 rounded-lg bg-rose-500/10 border border-rose-500/20 text-rose-300">
                                <span className="font-bold block mb-1">Diagnostic Error Message:</span>
                                <span>{evt.errorMessage}</span>
                              </div>
                            )}

                            <div>
                              <span className="text-[11px] text-slate-400 font-semibold block mb-1">Event Payload (JSON):</span>
                              <pre className="p-3 rounded-lg bg-slate-900 text-indigo-200 font-mono text-[11px] overflow-x-auto border border-slate-800">
                                {JSON.stringify(evt.payload, null, 2)}
                              </pre>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};
