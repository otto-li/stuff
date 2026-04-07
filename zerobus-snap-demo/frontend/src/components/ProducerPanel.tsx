import { useState } from "react";
import { ProducerStats, ProducerStateValue } from "../types";

interface Props {
  stats: ProducerStats;
  onStart: (rate: number) => void;
  onStop: () => void;
  onKill: () => void;
  onResume: () => void;
}

const STATE_CONFIG: Record<
  ProducerStateValue,
  { label: string; color: string; glow: string; pulse: boolean }
> = {
  STOPPED: {
    label: "STOPPED",
    color: "text-muted border-muted",
    glow: "",
    pulse: false,
  },
  RUNNING: {
    label: "● RUNNING",
    color: "text-neon-green border-neon-green",
    glow: "shadow-neon-green",
    pulse: true,
  },
  KILLED: {
    label: "☠ KILLED",
    color: "text-neon-red border-neon-red",
    glow: "shadow-neon-red",
    pulse: true,
  },
  RECONNECTING: {
    label: "↺ RECONNECTING",
    color: "text-neon-yellow border-neon-yellow",
    glow: "",
    pulse: false,
  },
};

export default function ProducerPanel({ stats, onStart, onStop, onKill, onResume }: Props) {
  const [rate, setRate] = useState(5);
  const cfg = STATE_CONFIG[stats.state];
  const isRunning = stats.state === "RUNNING";
  const isKilled = stats.state === "KILLED";
  const isStopped = stats.state === "STOPPED";
  const isReconnecting = stats.state === "RECONNECTING";

  return (
    <div className="panel h-full flex flex-col gap-4">
      <p className="panel-title">Producer Control</p>

      {/* State badge */}
      <div
        className={`text-center py-2 rounded border text-xs font-black tracking-widest ${cfg.color} ${cfg.glow} ${
          cfg.pulse && stats.state === "KILLED" ? "animate-kill-pulse" : ""
        }`}
      >
        {cfg.label}
      </div>

      {/* Zerobus config info */}
      <div className="text-[9px] text-text-dim space-y-1 font-mono bg-panel-light rounded p-2 border border-border">
        <div className="flex justify-between">
          <span className="text-muted">TABLE</span>
          <span className="text-text-dim truncate ml-2 text-right text-[8px]">ol.snap.game_events</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted">RATE</span>
          <span className="text-cyan tabular-nums">{stats.rate} ev/s</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted">SEQ#</span>
          <span className="text-cyan tabular-nums">{stats.sequence_num.toLocaleString()}</span>
        </div>
      </div>

      {/* Rate slider */}
      <div className="space-y-2">
        <div className="flex justify-between text-[9px] text-text-dim">
          <span className="tracking-widest">INGEST RATE</span>
          <span className="text-cyan font-black tabular-nums">{rate} ev/s</span>
        </div>
        <input
          type="range"
          min={1}
          max={100}
          value={rate}
          onChange={(e) => setRate(Number(e.target.value))}
          className="w-full accent-cyan h-1 rounded cursor-pointer"
        />
        <div className="flex justify-between text-[9px] text-muted">
          <span>1</span>
          <span>100</span>
        </div>
      </div>

      {/* Primary buttons: Start / Stop */}
      <div className="grid grid-cols-2 gap-2">
        <button
          className={`btn-green text-center ${!isStopped ? "btn-disabled" : ""}`}
          onClick={() => onStart(rate)}
          disabled={!isStopped}
        >
          ▶ START
        </button>
        <button
          className={`btn-red text-center ${!isRunning ? "btn-disabled" : ""}`}
          onClick={onStop}
          disabled={!isRunning}
        >
          ■ STOP
        </button>
      </div>

      {/* Crash / Resume buttons */}
      <div className="grid grid-cols-2 gap-2">
        <button
          className={`btn-red text-center ${!isRunning ? "btn-disabled" : ""}`}
          onClick={onKill}
          disabled={!isRunning}
        >
          ☠ CRASH
        </button>
        <button
          className={`btn-cyan text-center ${!isKilled ? "btn-disabled" : ""}`}
          onClick={onResume}
          disabled={!isKilled}
        >
          ↺ RESUME
        </button>
      </div>

      {/* Reconnecting spinner */}
      {isReconnecting && (
        <div className="flex items-center gap-2 text-[9px] text-neon-yellow">
          <div className="w-3 h-3 border border-neon-yellow border-t-transparent rounded-full animate-spin" />
          Reconnecting to WAL...
        </div>
      )}

      {/* Kill stats (visible after crash) */}
      {isKilled && stats.acked_at_kill > 0 && (
        <div className="bg-neon-red/5 border border-neon-red/20 rounded p-2 space-y-1">
          <p className="text-[9px] text-neon-red tracking-widest font-black">CRASH SNAPSHOT</p>
          <div className="text-[9px] font-mono space-y-0.5">
            <div className="flex justify-between">
              <span className="text-text-dim">Acked (durable)</span>
              <span className="text-neon-green tabular-nums">{stats.acked_at_kill}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-text-dim">In-flight at crash</span>
              <span className="text-neon-red tabular-nums">{stats.unacked_at_kill}</span>
            </div>
          </div>
        </div>
      )}

      {/* After-resume stats */}
      {stats.events_resent > 0 && (
        <div className="bg-cyan/5 border border-cyan/20 rounded p-2 space-y-1">
          <p className="text-[9px] text-cyan tracking-widest font-black">AT-LEAST-ONCE</p>
          <div className="text-[9px] font-mono space-y-0.5">
            <div className="flex justify-between">
              <span className="text-text-dim">Re-sent on resume</span>
              <span className="text-cyan tabular-nums">{stats.events_resent}</span>
            </div>
            <div className="text-[9px] text-text-dim mt-1">
              Dedup: SELECT DISTINCT event_id
            </div>
          </div>
        </div>
      )}

      {/* Last error (if any) */}
      {stats.last_error && (
        <div className="bg-neon-red/5 border border-neon-red/20 rounded p-2">
          <p className="text-[8px] text-neon-red tracking-widest font-black mb-1">LAST ERROR</p>
          <p className="text-[8px] text-text-dim font-mono break-all leading-relaxed">
            {stats.last_error.slice(0, 120)}
          </p>
        </div>
      )}
    </div>
  );
}
