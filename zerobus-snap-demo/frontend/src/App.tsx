import { useState, useEffect, useRef, useCallback } from "react";
import { ProducerStats, DEFAULT_STATS } from "./types";
import ProducerPanel from "./components/ProducerPanel";
import MetricsTicker from "./components/MetricsTicker";
import EventLog from "./components/EventLog";
import DeltaPanel from "./components/DeltaPanel";
import DurabilityProof from "./components/DurabilityProof";
import RejectionPanel from "./components/RejectionPanel";
import CodeModal from "./components/CodeModal";

export default function App() {
  const [stats, setStats] = useState<ProducerStats>(DEFAULT_STATS);
  const [showCode, setShowCode] = useState(false);
  const [wsConnected, setWsConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const connectWS = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${proto}//${window.location.host}/ws/producer`);
    wsRef.current = ws;

    ws.onopen = () => setWsConnected(true);
    ws.onmessage = (e: MessageEvent) => {
      try {
        setStats(JSON.parse(e.data) as ProducerStats);
      } catch {}
    };
    ws.onclose = () => {
      setWsConnected(false);
      reconnectTimer.current = setTimeout(connectWS, 2000);
    };
    ws.onerror = () => {
      ws.close();
    };
  }, []);

  useEffect(() => {
    connectWS();
    return () => {
      wsRef.current?.close();
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
    };
  }, [connectWS]);

  const api = useCallback(async (path: string, body?: object) => {
    try {
      await fetch(`/api/producer/${path}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: body ? JSON.stringify(body) : undefined,
      });
    } catch (e) {
      console.error("API error:", e);
    }
  }, []);

  const wasKilled = stats.acked_at_kill > 0;
  const showRejection = stats.rejection_count > 0;

  return (
    <div className="min-h-screen bg-void text-white font-orbitron select-none">
      {/* ── Header ─────────────────────────────────────────────────────────── */}
      <header className="border-b border-border px-6 py-3 flex items-center justify-between">
        <div>
          <h1 className="text-xl font-black tracking-[0.2em] text-cyan neon-text-cyan">
            ZEROBUS INGEST
          </h1>
          <p className="text-[9px] tracking-[0.35em] text-gold mt-0.5 neon-text-gold">
            MARVEL SNAP EDITION&nbsp;•&nbsp;POWERED BY DATABRICKS
          </p>
        </div>

        <div className="flex items-center gap-6">
          <div
            className={`flex items-center gap-2 text-[9px] tracking-widest ${
              wsConnected ? "text-neon-green" : "text-neon-red"
            }`}
          >
            <div
              className={`w-1.5 h-1.5 rounded-full ${
                wsConnected ? "bg-neon-green animate-pulse" : "bg-neon-red"
              }`}
            />
            {wsConnected ? "WS LIVE" : "CONNECTING"}
          </div>
          <span className="text-[9px] text-muted tracking-[0.25em]">SecondDinner</span>
        </div>
      </header>

      <main className="max-w-[1600px] mx-auto p-4">
        <div className="grid grid-cols-12 gap-4">

          {/* ── LEFT: Event Generator ──────────────────────────────────────── */}
          <div className="col-span-4 flex flex-col gap-4">
            <ProducerPanel
              stats={stats}
              onStart={(rate) => api("start", { rate })}
              onStop={() => api("stop")}
              onKill={() => api("kill")}
              onResume={() => api("resume")}
            />

            {/* Action buttons */}
            <div className="panel space-y-2">
              <p className="panel-title">Demo Actions</p>
              <button
                className={`btn-purple w-full ${stats.state !== "RUNNING" ? "btn-disabled" : ""}`}
                onClick={() => api("spike")}
                disabled={stats.state !== "RUNNING"}
              >
                ⚡ THROUGHPUT SPIKE
              </button>
              <button
                className={`btn-red w-full ${stats.state !== "RUNNING" ? "btn-disabled" : ""}`}
                onClick={() => api("schema-violation")}
                disabled={stats.state !== "RUNNING"}
              >
                ⚠ SCHEMA VIOLATION
              </button>
              <button className="btn-gold w-full" onClick={() => setShowCode(true)}>
                {"<>"} VIEW SDK CODE
              </button>
            </div>
          </div>

          {/* ── RIGHT: Monitoring ──────────────────────────────────────────── */}
          <div className="col-span-8 flex flex-col gap-4">
            {/* Metrics row */}
            <MetricsTicker stats={stats} />

            {/* Event log + Delta side by side */}
            <div className="grid grid-cols-12 gap-4">
              <div className="col-span-7">
                <EventLog events={stats.event_log} />
              </div>
              <div className="col-span-5">
                <DeltaPanel stats={stats} />
              </div>
            </div>

            {/* Durability proof (after kill) */}
            {wasKilled && <DurabilityProof stats={stats} />}

            {/* Rejection panel (after schema violation) */}
            {showRejection && <RejectionPanel stats={stats} />}
          </div>
        </div>

        {/* ── Footer ───────────────────────────────────────────────────────── */}
        <p className="text-[9px] text-muted tracking-widest text-center mt-4 pb-2">
          Zerobus WAL guarantees durability before acknowledgement&nbsp;•&nbsp;
          No message broker required&nbsp;•&nbsp;Direct Delta Lake writes
        </p>
      </main>

      {showCode && <CodeModal onClose={() => setShowCode(false)} />}
    </div>
  );
}
