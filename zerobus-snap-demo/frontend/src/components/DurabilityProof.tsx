import { useState, useEffect, useRef } from "react";
import { ProducerStats } from "../types";

interface Props {
  stats: ProducerStats;
}

export default function DurabilityProof({ stats }: Props) {
  const [deltaCount, setDeltaCount] = useState(stats.delta_count);
  const [proven, setProven] = useState(false);
  const [checking, setChecking] = useState(true);
  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const acked = stats.acked_at_kill;

  const poll = async () => {
    try {
      if (stats.demo_mode) {
        setDeltaCount(stats.delta_count);
        if (stats.delta_count >= acked && acked > 0) {
          setProven(true);
          setChecking(false);
          if (pollRef.current) clearInterval(pollRef.current);
        }
      } else {
        const r = await fetch("/api/delta/count");
        const data = await r.json();
        const cnt = data.count ?? 0;
        setDeltaCount(cnt);
        if (cnt >= acked && acked > 0) {
          setProven(true);
          setChecking(false);
          if (pollRef.current) clearInterval(pollRef.current);
        }
      }
    } catch {}
  };

  useEffect(() => {
    if (acked === 0) return;
    poll();
    pollRef.current = setInterval(poll, 2000);
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [acked, stats.delta_count]); // eslint-disable-line react-hooks/exhaustive-deps

  if (acked === 0) return null;

  const gap = acked - deltaCount;
  const pct = acked > 0 ? Math.min(100, Math.round((deltaCount / acked) * 100)) : 0;

  return (
    <div
      className={`panel border-2 ${
        proven ? "border-neon-green shadow-neon-green" : "border-neon-red/40"
      } transition-all duration-500`}
    >
      <div className="flex items-center justify-between mb-3">
        <p className="panel-title mb-0 text-neon-red">
          {proven ? "✓ DURABILITY PROOF" : "⟳ VERIFYING DURABILITY"}
        </p>
        {checking && !proven && (
          <div className="flex items-center gap-2 text-[9px] text-neon-yellow">
            <div className="w-3 h-3 border border-neon-yellow border-t-transparent rounded-full animate-spin" />
            Polling Delta...
          </div>
        )}
      </div>

      <div className="grid grid-cols-3 gap-4 text-center">
        <div className="stat-box">
          <div className="stat-value text-neon-red tabular-nums">{acked.toLocaleString()}</div>
          <div className="stat-label">WAL-ACKED BEFORE CRASH</div>
        </div>
        <div className="stat-box">
          <div className={`stat-value tabular-nums ${proven ? "text-neon-green" : "text-neon-yellow"}`}>
            {deltaCount.toLocaleString()}
          </div>
          <div className="stat-label">IN DELTA TABLE NOW</div>
        </div>
        <div className="stat-box">
          <div className={`stat-value tabular-nums ${proven ? "text-neon-green neon-text-green" : "text-neon-yellow"}`}>
            {proven ? "0" : gap.toLocaleString()}
          </div>
          <div className="stat-label">GAP (LOST EVENTS)</div>
        </div>
      </div>

      {/* Progress bar */}
      <div className="mt-4 space-y-1">
        <div className="flex justify-between text-[9px] text-text-dim">
          <span>Materialization progress</span>
          <span className="tabular-nums">{pct}%</span>
        </div>
        <div className="h-2 bg-panel-light rounded overflow-hidden">
          <div
            className={`h-full rounded transition-all duration-500 ${
              proven ? "bg-neon-green" : "bg-neon-yellow"
            }`}
            style={{ width: `${pct}%` }}
          />
        </div>
      </div>

      {proven && (
        <div className="mt-4 text-center">
          <div className="text-2xl font-black text-neon-green neon-text-green tracking-widest">
            ZERO DATA LOSS
          </div>
          <p className="text-[9px] text-text-dim mt-1 tracking-widest">
            Zerobus WAL committed all {acked.toLocaleString()} events before crash acknowledgement.
            Delta table recovered {deltaCount.toLocaleString()} rows.
          </p>
        </div>
      )}

      {/* Explanation */}
      <div className="mt-3 bg-panel-light rounded p-2 text-[9px] text-text-dim font-mono space-y-0.5">
        <p className="text-text-dim">
          <span className="text-cyan">How it works:</span> Zerobus writes to a WAL (Write-Ahead Log)
          before returning an ack. Even on hard crash, all WAL-committed records are materialized
          into Delta by the background Zerobus worker.
        </p>
      </div>
    </div>
  );
}
