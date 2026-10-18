'use client';

import React, { useState } from 'react';
import { 
  GitCommit, 
  Clock, 
  Layers, 
  Database, 
  Zap, 
  Radio, 
  ShieldCheck, 
  ChevronRight, 
  CheckCircle2, 
  Info,
  Server
} from 'lucide-react';
import { ProcessedEventRecord, TraceSpan } from '@/lib/types';

interface TraceWaterfallProps {
  events: ProcessedEventRecord[];
}

export const TraceWaterfall: React.FC<TraceWaterfallProps> = ({ events }) => {
  const [selectedEventId, setSelectedEventId] = useState<string>(events[0]?.id || '');
  const selectedEvent = events.find(e => e.id === selectedEventId) || events[0];

  const getServiceColor = (service: TraceSpan['service']) => {
    switch (service) {
      case 'HTTP_CONTROLLER':
        return 'text-blue-400 bg-blue-500/10 border-blue-500/20';
      case 'KAFKA_BROKER':
        return 'text-amber-400 bg-amber-500/10 border-amber-500/20';
      case 'REDIS_CACHE':
        return 'text-emerald-400 bg-emerald-500/10 border-emerald-500/20';
      case 'POSTGRES_DB':
        return 'text-indigo-400 bg-indigo-500/10 border-indigo-500/20';
      case 'BUSINESS_EXECUTOR':
        return 'text-purple-400 bg-purple-500/10 border-purple-500/20';
      default:
        return 'text-slate-400 bg-slate-500/10 border-slate-500/20';
    }
  };

  const getServiceIcon = (service: TraceSpan['service']) => {
    switch (service) {
      case 'HTTP_CONTROLLER':
        return Radio;
      case 'KAFKA_BROKER':
        return Layers;
      case 'REDIS_CACHE':
        return Zap;
      case 'POSTGRES_DB':
        return Database;
      case 'BUSINESS_EXECUTOR':
        return Server;
      default:
        return GitCommit;
    }
  };

  const totalDuration = selectedEvent?.durationMs || 50;

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm">
      <div className="flex flex-col md:flex-row md:items-center justify-between pb-4 sm:pb-6 border-b border-slate-800 gap-4">
        <div>
          <div className="flex items-center gap-2">
            <h2 className="text-sm sm:text-base font-bold text-white flex items-center gap-2">
              <Clock className="w-4 h-4 sm:w-5 sm:h-5 text-indigo-400 shrink-0" />
              <span>Distributed Trace Waterfall (MDC Tracing)</span>
            </h2>
            <span className="hidden sm:inline-flex text-[11px] px-2.5 py-0.5 rounded-full bg-indigo-500/10 text-indigo-400 border border-indigo-500/20 font-mono">
              Trace Propagation
            </span>
          </div>
          <p className="text-[11px] sm:text-xs text-slate-400 mt-1">
            Follow the exact lifecycle of an event across HTTP, Kafka partition offsets, Redis fast-path, and PostgreSQL ACID transactions.
          </p>
        </div>

        {/* Event selector dropdown */}
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <span className="text-slate-400 font-semibold text-[11px] sm:text-xs">Select Event:</span>
          <select
            value={selectedEvent?.id}
            onChange={e => setSelectedEventId(e.target.value)}
            className="w-full sm:w-auto bg-slate-950 border border-slate-800 rounded-xl px-3 py-1.5 text-white font-mono text-xs focus:outline-none focus:border-indigo-500 cursor-pointer"
          >
            {events.map(evt => (
              <option key={evt.id} value={evt.id}>
                {evt.eventId} ({evt.deduplicationSource === 'NONE' ? '1st Run' : evt.deduplicationSource}) - {evt.durationMs}ms
              </option>
            ))}
          </select>
        </div>
      </div>

      {selectedEvent ? (
        <div className="mt-6 space-y-6">
          {/* Summary metadata bar */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 bg-slate-950 p-4 rounded-xl border border-slate-800 text-xs font-mono">
            <div>
              <span className="text-slate-500 block text-[11px] font-sans">Trace ID</span>
              <span className="text-indigo-300 font-bold">{selectedEvent.traceId}</span>
            </div>
            <div>
              <span className="text-slate-500 block text-[11px] font-sans">Fencing Token</span>
              <span className="text-emerald-400 truncate block max-w-[120px]">{selectedEvent.processingToken}</span>
            </div>
            <div>
              <span className="text-slate-500 block text-[11px] font-sans">Execution Path</span>
              <span className="text-slate-200">{selectedEvent.deduplicationSource}</span>
            </div>
            <div>
              <span className="text-slate-500 block text-[11px] font-sans">Total Latency</span>
              <span className="text-amber-400 font-bold">{selectedEvent.durationMs}ms</span>
            </div>
          </div>

          {/* Waterfall Spans Timeline */}
          <div className="space-y-3 font-mono text-xs">
            {selectedEvent.spans && selectedEvent.spans.length > 0 ? (
              selectedEvent.spans.map((span) => {
                const Icon = getServiceIcon(span.service);
                const widthPercent = Math.max(8, (span.durationMs / totalDuration) * 100);
                const leftPercent = Math.min(85, (span.offsetMs / totalDuration) * 100);

                return (
                  <div key={span.id} className="p-3 bg-slate-950/60 rounded-xl border border-slate-800/80 hover:border-slate-700 transition">
                    <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2 mb-2 font-sans">
                      <div className="flex items-center gap-2">
                        <span className={`px-2 py-0.5 rounded-md text-[10px] font-semibold border ${getServiceColor(span.service)} flex items-center gap-1`}>
                          <Icon className="w-3 h-3" />
                          {span.service}
                        </span>
                        <span className="text-white font-medium text-xs font-mono">{span.name}</span>
                      </div>
                      <div className="flex items-center gap-3 text-xs font-mono text-slate-400">
                        <span className="text-slate-300 font-bold">+{span.durationMs}ms</span>
                        <span className="text-[11px] text-slate-500">offset: {span.offsetMs}ms</span>
                      </div>
                    </div>

                    {/* Timeline Gantt Bar */}
                    <div className="w-full bg-slate-900 h-2.5 rounded-full relative overflow-hidden my-2">
                      <div
                        className="h-full rounded-full bg-gradient-to-r from-indigo-500 to-emerald-400 transition-all duration-500"
                        style={{
                          marginLeft: `${leftPercent}%`,
                          width: `${widthPercent}%`,
                        }}
                      />
                    </div>

                    {/* Span Details text */}
                    <p className="text-[11px] text-slate-400 font-sans mt-1">
                      {span.details}
                    </p>
                  </div>
                );
              })
            ) : (
              <div className="text-center py-8 text-slate-500 font-sans">
                No trace spans recorded for this event.
              </div>
            )}
          </div>
        </div>
      ) : null}
    </div>
  );
};
