import { useState, useEffect, useRef } from "react";
import { ProducerStats } from "../types";

interface Props {
  stats: ProducerStats;
}

const TYPE_BAR_COLORS: Record<string, string> = {
  match_started: "bg-cyan",
  card_played: "bg-gold",
  snap_triggered: "bg-neon-purple",
  match_ended: "bg-neon-green",
};

export default function DeltaPanel({ stats }: Props) {
  const [deltaCount, setDeltaCount] = useState(0);
  const [prevCount, setPrevCount] = useState(0);
  const [lastRefresh, setLastRefresh] = useState(Date.now());
  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const isFirstMount = useRef(true);

  const fetchCount = async () => {
    try {
      const r = await fetch("/api/delta/count");
      const data = await r.json();
      setDeltaCount((prev) => {
        setPrevCount(prev);
        return data.count ?? 0;
      });
      setLastRefresh(Date.now());
    } catch {}
  };

  useEffect(() => {
    if (isFirstMount.current) {
      isFirstMount.current = false;
      fetchCount();
    }
    pollRef.current = setInterval(fetchCount, 3000);
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, []);

  const delta = Math.max(0, deltaCount - prevCount);
  const breakdown = Object.entries(stats.delta_by_type).sort(([, a], [, b]) => b - a);
  const maxCount = breakdown.length > 0 ? breakdown[0][1] : 1;
  const secondsAgo = Math.floor((Date.now() - lastRefresh) / 1000);

  return (
    <div className="panel">
      <div className="flex items-center justify-between mb-3">
        <p className="panel-title mb-0">Delta Table</p>
        <span className="text-[8px] text-muted">{secondsAgo}s ago</span>
      </div>

      {/* Big count badge */}
      <div className="bg-panel-light rounded-lg p-4 text-center border border-border mb-3">
        <div className="text-4xl font-black text-neon-green neon-text-green tabular-nums">
          {deltaCount.toLocaleString()}
        </div>
        <div className="text-[9px] text-text-dim tracking-[0.2em] mt-1">ROWS IN DELTA</div>
        {delta > 0 && (
          <div className="text-[9px] text-neon-green mt-1">▲ +{delta} since last refresh</div>
        )}
      </div>

      {/* Event type breakdown */}
      {breakdown.length > 0 && (
        <div className="space-y-2">
          <p className="text-[8px] text-muted tracking-widest">BY EVENT TYPE</p>
          {breakdown.map(([type, count]) => (
            <div key={type} className="space-y-0.5">
              <div className="flex justify-between text-[9px]">
                <span className="font-mono text-text-dim">
                  {type.replace("_", " ")}
                </span>
                <span className="text-text-dim tabular-nums">{count.toLocaleString()}</span>
              </div>
              <div className="h-1 bg-panel-light rounded overflow-hidden">
                <div
                  className={`h-full rounded transition-all duration-500 ${
                    TYPE_BAR_COLORS[type] ?? "bg-muted"
                  }`}
                  style={{ width: `${Math.round((count / maxCount) * 100)}%` }}
                />
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Auto-refresh indicator */}
      <div className="flex items-center gap-1.5 mt-3 text-[8px] text-muted">
        <div className="w-1.5 h-1.5 rounded-full bg-neon-green animate-pulse" />
        Auto-refresh every 3s
      </div>
    </div>
  );
}
