'use client';

import React, { useState } from 'react';
import { Database, CreditCard, ShoppingBag, CheckCircle, ShieldCheck, Hash } from 'lucide-react';
import { OrderRecord, PaymentRecord } from '@/lib/types';

interface BusinessRecordsViewerProps {
  payments: PaymentRecord[];
  orders: OrderRecord[];
}

export const BusinessRecordsViewer: React.FC<BusinessRecordsViewerProps> = ({ payments, orders }) => {
  const [activeTab, setActiveTab] = useState<'payments' | 'orders'>('payments');

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-2xl p-4 sm:p-6 shadow-sm">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-4 sm:pb-6 border-b border-slate-800 gap-4">
        <div>
          <div className="flex items-center gap-2">
            <h2 className="text-sm sm:text-base font-bold text-white flex items-center gap-2">
              <Database className="w-4 h-4 sm:w-5 sm:h-5 text-indigo-400 shrink-0" />
              <span>PostgreSQL Business Entities</span>
            </h2>
            <span className="hidden sm:inline-flex px-2.5 py-0.5 rounded-full text-[11px] font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 items-center gap-1">
              <ShieldCheck className="w-3.5 h-3.5" /> Exactly-Once Verified
            </span>
          </div>
          <p className="text-[11px] sm:text-xs text-slate-400 mt-1">
            Persisted business state in PostgreSQL. Even if an event was retried 10 times, exactly 1 business mutation exists here.
          </p>
        </div>

        {/* Tab switch */}
        <div className="flex items-center bg-slate-950 p-1 rounded-xl border border-slate-800 text-xs self-start sm:self-auto">
          <button
            onClick={() => setActiveTab('payments')}
            className={`px-3 py-1.5 rounded-lg font-medium transition flex items-center gap-1.5 ${
              activeTab === 'payments'
                ? 'bg-indigo-600 text-white shadow-sm'
                : 'text-slate-400 hover:text-white'
            }`}
          >
            <CreditCard className="w-3.5 h-3.5" />
            <span>Payments ({payments.length})</span>
          </button>
          <button
            onClick={() => setActiveTab('orders')}
            className={`px-3 py-1.5 rounded-lg font-medium transition flex items-center gap-1.5 ${
              activeTab === 'orders'
                ? 'bg-indigo-600 text-white shadow-sm'
                : 'text-slate-400 hover:text-white'
            }`}
          >
            <ShoppingBag className="w-3.5 h-3.5" />
            <span>Orders ({orders.length})</span>
          </button>
        </div>
      </div>

      {/* Content */}
      <div className="mt-4">
        {activeTab === 'payments' ? (
          <div className="overflow-x-auto no-scrollbar">
            <table className="w-full min-w-[620px] text-left text-xs font-mono">
              <thead>
                <tr className="border-b border-slate-800 text-slate-400 font-semibold uppercase tracking-wider font-sans">
                  <th className="py-3 px-3"># ID</th>
                  <th className="py-3 px-3">Payment ID (Unique)</th>
                  <th className="py-3 px-3">User ID</th>
                  <th className="py-3 px-3 text-right">Amount</th>
                  <th className="py-3 px-3">Currency</th>
                  <th className="py-3 px-3">Persisted At</th>
                  <th className="py-3 px-3 text-center">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60">
                {payments.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="text-center py-8 text-slate-500 font-sans">
                      No payment entities persisted yet. Submit a Payment event in the Simulator.
                    </td>
                  </tr>
                ) : (
                  payments.map(p => (
                    <tr key={p.id} className="hover:bg-slate-800/40 transition">
                      <td className="py-3 px-3 text-slate-500">{p.id}</td>
                      <td className="py-3 px-3 font-semibold text-white">{p.paymentId}</td>
                      <td className="py-3 px-3 text-indigo-300">{p.userId}</td>
                      <td className="py-3 px-3 text-right font-bold text-emerald-400">
                        ${p.amount.toFixed(2)}
                      </td>
                      <td className="py-3 px-3 text-slate-300">{p.currency}</td>
                      <td className="py-3 px-3 text-slate-400 text-[11px]">{p.createdAt}</td>
                      <td className="py-3 px-3 text-center">
                        <span className="inline-flex items-center gap-1 text-[11px] font-sans font-medium text-emerald-400">
                          <CheckCircle className="w-3.5 h-3.5" /> Committed
                        </span>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="overflow-x-auto no-scrollbar">
            <table className="w-full min-w-[620px] text-left text-xs font-mono">
              <thead>
                <tr className="border-b border-slate-800 text-slate-400 font-semibold uppercase tracking-wider font-sans">
                  <th className="py-3 px-3"># ID</th>
                  <th className="py-3 px-3">Order ID (Unique)</th>
                  <th className="py-3 px-3">Customer ID</th>
                  <th className="py-3 px-3 text-right">Total Amount</th>
                  <th className="py-3 px-3 text-center">Item Count</th>
                  <th className="py-3 px-3">Persisted At</th>
                  <th className="py-3 px-3 text-center">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60">
                {orders.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="text-center py-8 text-slate-500 font-sans">
                      No order entities persisted yet. Submit an Order event in the Simulator.
                    </td>
                  </tr>
                ) : (
                  orders.map(o => (
                    <tr key={o.id} className="hover:bg-slate-800/40 transition">
                      <td className="py-3 px-3 text-slate-500">{o.id}</td>
                      <td className="py-3 px-3 font-semibold text-white">{o.orderId}</td>
                      <td className="py-3 px-3 text-indigo-300">{o.customerId}</td>
                      <td className="py-3 px-3 text-right font-bold text-emerald-400">
                        ${o.totalAmount.toFixed(2)}
                      </td>
                      <td className="py-3 px-3 text-center text-slate-300">{o.itemCount}</td>
                      <td className="py-3 px-3 text-slate-400 text-[11px]">{o.createdAt}</td>
                      <td className="py-3 px-3 text-center">
                        <span className="inline-flex items-center gap-1 text-[11px] font-sans font-medium text-emerald-400">
                          <CheckCircle className="w-3.5 h-3.5" /> Committed
                        </span>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Info callout */}
      <div className="mt-5 p-3.5 rounded-xl bg-slate-950 border border-slate-800 flex items-center justify-between text-xs text-slate-400">
        <span>PostgreSQL Unique Constraints: <code>uq_payments_payment_id</code> & <code>uq_orders_order_id</code></span>
        <span className="text-emerald-400 font-semibold flex items-center gap-1">
          <CheckCircle className="w-3.5 h-3.5" /> 0 Orphaned Records
        </span>
      </div>
    </div>
  );
};
