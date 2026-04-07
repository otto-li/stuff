import { ProducerStats } from "../types";

interface Props {
  stats: ProducerStats;
}

function StatBox({
  value,
  label,
  color = "text-white",
  glow = "",
}: {
  value: string | number;
  label: string;
  color?: string;
  glow?: string;
}) {
  return (
    <div className="stat-box flex-1">
      <div className={`stat-value ${color} ${glow} tabular-nums`}>{value}</div>
      <div className="stat-label">{label}</div>
    </div>
  );
}

export default function MetricsTicker({ stats }: Props) {
  const ackPct =
    stats.events_sent > 0
      ? ((stats.events_acked / stats.events_sent) * 100).toFixed(1)
      : "0.0";
  const failPct =
    stats.events_sent > 0
      ? ((stats.events_failed / stats.events_sent) * 100).toFixed(2)
      : "0.00";

  return (
    <div className="panel">
      <p className="panel-title">Live Metrics</p>
      <div className="flex gap-3">
        <StatBox
          value={stats.events_sent.toLocaleString()}
          label="EVENTS SENT"
          color="text-white"
        />
        <StatBox
          value={stats.events_acked.toLocaleString()}
          label="WAL ACKED"
          color="text-neon-green"
          glow="neon-text-green"
        />
        <StatBox
          value={stats.events_in_flight}
          label="IN-FLIGHT"
          color="text-cyan"
          glow="neon-text-cyan"
        />
        <StatBox
          value={`${stats.events_per_sec.toFixed(1)}/s`}
          label="THROUGHPUT"
          color="text-gold"
          glow="neon-text-gold"
        />
        <StatBox
          value={`${ackPct}%`}
          label="ACK RATE"
          color="text-neon-green"
        />
        <StatBox
          value={`${failPct}%`}
          label="FAIL RATE"
          color={stats.events_failed > 0 ? "text-neon-red" : "text-text-dim"}
        />
        <StatBox
          value={stats.rejection_count}
          label="REJECTIONS"
          color={stats.rejection_count > 0 ? "text-neon-red" : "text-text-dim"}
        />
      </div>
    </div>
  );
}
