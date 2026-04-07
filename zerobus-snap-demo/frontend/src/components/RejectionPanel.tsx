import { useState } from "react";
import { ProducerStats } from "../types";

interface Props {
  stats: ProducerStats;
}

const REJECTION_SQL = `-- Anomalous rows: NULL event_id, unknown event types,
-- or sequence numbers that appear > once (at-least-once resends)

SELECT
  event_id,
  event_type,
  sequence_num,
  from_unixtime(produced_at / 1e6) AS produced_at,
  COUNT(*) OVER (PARTITION BY event_id) AS times_written
FROM ol.snap.game_events
WHERE event_id IS NULL
   OR event_type NOT IN (
        'match_started','card_played',
        'snap_triggered','match_ended')
   OR sequence_num IS NULL

UNION ALL

-- Duplicate event_ids from crash+resume (at-least-once)
SELECT e.event_id, e.event_type, e.sequence_num,
       from_unixtime(e.produced_at / 1e6), cnt AS times_written
FROM ol.snap.game_events e
JOIN (
  SELECT event_id, COUNT(*) AS cnt
  FROM ol.snap.game_events
  GROUP BY event_id HAVING cnt > 1
) d ON e.event_id = d.event_id
ORDER BY sequence_num;`;

export default function RejectionPanel({ stats }: Props) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(REJECTION_SQL).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  return (
    <div className="panel border border-neon-red/30">
      <p className="panel-title text-neon-red">Schema Rejections</p>

      <div className="flex items-center gap-3">
        <div className="text-3xl font-black text-neon-red neon-text-red tabular-nums">
          {stats.rejection_count}
        </div>
        <div className="text-[9px] text-text-dim tracking-widest">
          REJECTED<br />RECORDS
        </div>
      </div>

      <div className="mt-3 bg-panel-light rounded p-2 text-[9px] space-y-1 border border-border">
        <p className="text-text-dim leading-relaxed">
          Tracked via SDK <span className="font-mono text-neon-red">on_error()</span> callback.
          Zerobus writes server-side rejections to{" "}
          <span className="font-mono text-muted">_zerobus/table_rejected_parquets/</span>{" "}
          inside the table storage — queryable via{" "}
          <span className="font-mono text-muted">read_files()</span> on external tables only
          (blocked on UC managed storage).
        </p>
      </div>

      {/* SQL snippet with copy button */}
      <div className="mt-3 bg-panel-light rounded border border-border overflow-hidden">
        <div className="flex items-center justify-between px-2 py-1.5 border-b border-border">
          <span className="text-[8px] text-gold tracking-widest font-black">
            SQL — ANOMALOUS &amp; DUPLICATE ROWS
          </span>
          <button
            onClick={handleCopy}
            className={`text-[8px] px-2 py-0.5 rounded border transition-colors ${
              copied
                ? "border-neon-green text-neon-green"
                : "border-muted text-muted hover:border-cyan hover:text-cyan"
            }`}
          >
            {copied ? "✓ COPIED" : "COPY"}
          </button>
        </div>
        <pre className="text-[8px] font-mono text-text-dim leading-relaxed p-2 overflow-x-auto whitespace-pre">
          {REJECTION_SQL}
        </pre>
      </div>
    </div>
  );
}
