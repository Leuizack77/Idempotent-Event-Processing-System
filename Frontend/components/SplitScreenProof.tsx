'use client';

import React, { useState } from 'react';
import { 
  Split, 
  Send, 
  ShieldCheck, 
  Database, 
  Layers, 
  CheckCircle2, 
  Sparkles, 
  ArrowRight,
  ArrowDown,
  Zap,
  Repeat
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { eventService } from '@/lib/api';
import { OrderRecord, PaymentRecord, ProcessedEventRecord } from '@/lib/types';

interface SplitScreenProofProps {
  events: ProcessedEventRecord[];
  payments: PaymentRecord[];
  orders: OrderRecord[];
  onRefresh: () => void;
}

export const SplitScreenProof: React.FC<SplitScreenProofProps> = ({ events, payments, orders, onRefresh }) => {
  const [selectedEventId, setSelectedEventId] = useState<string>('PAY-TXN-8812');
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);

  // Filter events matching the selected eventId
  const matchingEvents = events.filter(e => e.eventId === selectedEventId);
  const matchingPayment = payments.find(p => p.paymentId === selectedEventId);
  const matchingOrder = orders.find(o => o.orderId === selectedEventId);

  const handleDuplicateClick = async () => {
    setIsSubmitting(true);
    try {
      await eventService.submitEvent({
        eventId: selectedEventId,
        eventType: selectedEventId.startsWith('ORD') ? 'ORDER_CREATED' : 'PAYMENT_PROCESSED',
        payload: {
          paymentId: selectedEventId,
          userId: 'user_al1ce',
          amount: 149.99,
          currency: 'USD',
        },
      });
      onRefresh();
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleNewTestScenario = () => {
    const newId = `PAY-TXN-${Math.floor(1000 + Math.random() * 9000)}`;
    setSelectedEventId(newId);
  };

  return (
    <div className="glass-panel rounded-2xl sm:rounded-3xl p-4 sm:p-6 shadow-xl relative overflow-hidden">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center justify-between pb-4 sm:pb-6 border-b border-slate-800/80 gap-4">
        <div>
          <div className="flex items-center gap-2">
            <h2 className="text-sm sm:text-base font-bold text-white flex items-center gap-2">
              <Split className="w-4 h-4 sm:w-5 sm:h-5 text-indigo-400 shrink-0" />
              <span>Side-by-Side Proof: At-Least-Once Delivery vs. Exactly-Once Effect</span>
            </h2>
            <span className="hidden sm:inline-flex text-[11px] px-2.5 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 font-semibold items-center gap-1 shadow-[0_0_12px_rgba(16,185,129,0.2)]">
              <ShieldCheck className="w-3.5 h-3.5" /> Zero Double Charging
            </span>
          </div>
          <p className="text-[11px] sm:text-xs text-slate-400 mt-1">
            Notice how every duplicate transport retry on the left is intercepted, leaving <strong>EXACTLY ONE</strong> persistent record on the right!
          </p>
        </div>

        {/* Action Controls */}
        <div className="flex flex-wrap items-center gap-2 sm:gap-3">
          <motion.button
            whileHover={{ scale: 1.03 }}
            whileTap={{ scale: 0.97 }}
            onClick={handleNewTestScenario}
            className="text-[11px] sm:text-xs px-3 sm:px-3.5 py-2 bg-slate-950/80 hover:bg-slate-800/80 text-slate-300 rounded-xl border border-slate-800 transition flex items-center gap-1.5 shadow-sm"
          >
            <Sparkles className="w-3.5 h-3.5 text-indigo-400" />
            <span>New Key</span>
          </motion.button>

          <motion.button
            whileHover={{ scale: 1.03 }}
            whileTap={{ scale: 0.97 }}
            onClick={handleDuplicateClick}
            disabled={isSubmitting}
            className="text-[11px] sm:text-xs px-3.5 sm:px-4 py-2 bg-gradient-to-r from-indigo-600 via-indigo-500 to-cyan-600 hover:from-indigo-500 hover:to-cyan-500 text-white font-bold rounded-xl shadow-lg shadow-indigo-600/25 border border-indigo-400/30 transition flex items-center gap-1.5 disabled:opacity-50"
          >
            <Repeat className="w-3.5 h-3.5" />
            <span>Duplicate for <span className="font-mono">{selectedEventId}</span></span>
          </motion.button>
        </div>
      </div>

      {/* Split-Screen Grid */}
      <div className="mt-4 sm:mt-6 grid grid-cols-1 lg:grid-cols-12 gap-4 sm:gap-6">
        {/* Left Side: Transport Ingestion Stream */}
        <div className="lg:col-span-6 bg-slate-950/80 p-4 sm:p-5 rounded-xl sm:rounded-2xl border border-slate-800/80 flex flex-col justify-between shadow-inner">
          <div>
            <div className="flex items-center justify-between pb-3 border-b border-slate-800/80">
              <div className="flex items-center gap-2">
                <Layers className="w-4 h-4 text-amber-400" />
                <h3 className="text-sm font-bold text-white">Transport Ingestion Log</h3>
              </div>
              <span className="text-xs font-mono px-2 py-0.5 bg-amber-500/10 text-amber-400 rounded-md border border-amber-500/20">
                At-Least-Once Delivery
              </span>
            </div>

            <p className="text-xs text-slate-400 mt-2">
              Kafka brokers & client retries can redeliver this record repeatedly:
            </p>

            <div className="mt-4 space-y-2.5 font-mono text-xs max-h-[340px] overflow-y-auto pr-1">
              <AnimatePresence>
                {matchingEvents.length === 0 ? (
                  <div className="p-6 text-center text-slate-500 text-xs font-sans">
                    Target key <code>{selectedEventId}</code> has not been submitted yet.
                    <br />
                    Click <strong>&quot;Send Duplicate&quot;</strong> above to start!
                  </div>
                ) : (
                  matchingEvents.map((evt, idx) => (
                    <motion.div
                      key={evt.id}
                      initial={{ opacity: 0, x: -10 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ duration: 0.25 }}
                      className={`p-3 rounded-xl border flex items-center justify-between ${
                        idx === matchingEvents.length - 1 && matchingEvents.length > 1
                          ? 'bg-indigo-500/15 border-indigo-500/40 text-indigo-300 shadow-[0_0_15px_rgba(99,102,241,0.2)]'
                          : 'bg-slate-900/80 border-slate-800 text-slate-300'
                      }`}
                    >
                      <div>
                        <div className="flex items-center gap-2">
                          <span className="text-[11px] font-bold text-slate-500">#{matchingEvents.length - idx}</span>
                          <span className="font-bold text-white text-xs">{evt.eventId}</span>
                          <span className="text-[10px] px-1.5 py-0.2 rounded bg-slate-800 text-slate-400">
                            {evt.deduplicationSource === 'NONE' ? '1st Delivery' : evt.deduplicationSource}
                          </span>
                        </div>
                        <div className="text-[11px] text-slate-500 mt-1 font-sans">
                          Token: {evt.processingToken.substring(0, 8)}... | Duration: {evt.durationMs}ms
                        </div>
                      </div>

                      <div className="text-right">
                        <span className="text-xs font-bold text-emerald-400 flex items-center gap-1">
                          <CheckCircle2 className="w-3.5 h-3.5" /> ACK
                        </span>
                        <span className="text-[10px] text-slate-500 block">{evt.createdAt.split('T')[1].substring(0, 8)}</span>
                      </div>
                    </motion.div>
                  ))
                )}
              </AnimatePresence>
            </div>
          </div>

          <div className="mt-4 pt-3 border-t border-slate-900 flex items-center justify-between text-xs text-slate-400">
            <span>Total Deliveries Received:</span>
            <span className="font-mono font-bold text-amber-400">{matchingEvents.length} requests</span>
          </div>
        </div>

        {/* Desktop Middle Connector Arrow */}
        <div className="hidden lg:flex items-center justify-center -mx-3 z-10">
          <div className="w-8 h-8 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center text-indigo-400 shadow-md">
            <ArrowRight className="w-4 h-4" />
          </div>
        </div>

        {/* Mobile Connector Arrow */}
        <div className="flex lg:hidden justify-center text-indigo-400 my-0.5">
          <div className="w-7 h-7 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center shadow-sm">
            <ArrowDown className="w-3.5 h-3.5" />
          </div>
        </div>

        {/* Right Side: Authoritative PostgreSQL Database Record */}
        <div className="lg:col-span-5 bg-slate-950 p-4 sm:p-5 rounded-xl sm:rounded-2xl border border-slate-800/80 flex flex-col justify-between">
          <div>
            <div className="flex items-center justify-between pb-3 border-b border-slate-800">
              <div className="flex items-center gap-2">
                <Database className="w-4 h-4 text-emerald-400" />
                <h3 className="text-sm font-bold text-white">PostgreSQL Business Table</h3>
              </div>
              <span className="text-xs font-mono px-2 py-0.5 bg-emerald-500/10 text-emerald-400 rounded-md border border-emerald-500/20 font-bold">
                Exactly-Once Effect
              </span>
            </div>

            <p className="text-xs text-slate-400 mt-2">
              Regardless of duplicate deliveries, the database enforces strict single-row uniqueness:
            </p>

            <div className="mt-4">
              {matchingPayment ? (
                <div className="bg-slate-900 p-4 rounded-xl border border-emerald-500/30 space-y-3 font-mono text-xs">
                  <div className="flex items-center justify-between">
                    <span className="text-slate-500 font-sans text-xs">Table: payments</span>
                    <span className="px-2 py-0.5 rounded-full bg-emerald-500/20 text-emerald-400 text-[11px] font-bold">
                      1 Row Stored
                    </span>
                  </div>

                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <div>
                      <span className="text-slate-500 block text-[10px] font-sans">Primary Key #</span>
                      <span className="text-white font-bold">{matchingPayment.id}</span>
                    </div>
                    <div>
                      <span className="text-slate-500 block text-[10px] font-sans">Payment ID (Unique)</span>
                      <span className="text-indigo-300 font-bold">{matchingPayment.paymentId}</span>
                    </div>
                    <div>
                      <span className="text-slate-500 block text-[10px] font-sans">Amount Charged</span>
                      <span className="text-emerald-400 font-bold">${matchingPayment.amount.toFixed(2)}</span>
                    </div>
                    <div>
                      <span className="text-slate-500 block text-[10px] font-sans">User ID</span>
                      <span className="text-slate-300">{matchingPayment.userId}</span>
                    </div>
                  </div>

                  <div className="pt-2 border-t border-slate-800 text-[11px] text-slate-400 font-sans">
                    Constraint <code>uq_payments_payment_id</code> guarantees this row can never be duplicated.
                  </div>
                </div>
              ) : matchingOrder ? (
                <div className="bg-slate-900 p-4 rounded-xl border border-emerald-500/30 space-y-3 font-mono text-xs">
                  <div className="flex items-center justify-between">
                    <span className="text-slate-500 font-sans text-xs">Table: orders</span>
                    <span className="px-2 py-0.5 rounded-full bg-emerald-500/20 text-emerald-400 text-[11px] font-bold">
                      1 Row Stored
                    </span>
                  </div>
                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <div>
                      <span className="text-slate-500 block text-[10px] font-sans">Order ID (Unique)</span>
                      <span className="text-indigo-300 font-bold">{matchingOrder.orderId}</span>
                    </div>
                    <div>
                      <span className="text-slate-500 block text-[10px] font-sans">Total Amount</span>
                      <span className="text-emerald-400 font-bold">${matchingOrder.totalAmount.toFixed(2)}</span>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="p-6 text-center text-slate-500 text-xs font-sans">
                  No row written to PostgreSQL yet. Submit an event first!
                </div>
              )}
            </div>
          </div>

          <div className="mt-4 pt-3 border-t border-slate-900 flex items-center justify-between text-xs">
            <span className="text-slate-400">Database Row Count:</span>
            <span className="font-mono font-bold text-emerald-400">
              {matchingPayment || matchingOrder ? 'EXACTLY 1' : '0'}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
};
