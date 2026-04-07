interface RetlData {
  synergy_cards: Array<{
    card_b: string;
    synergy_score: number | string;
    shared_tags: string;
    same_cluster: string;
  }>;
  source: string;
}

export default function RetlPanel({ data }: { data: RetlData | null }) {
  if (!data?.synergy_cards?.length) {
    return <div className="text-text-dim text-xs">No rETL results</div>;
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <div className="text-[9px] tracking-[0.2em] text-text-dim">
          TOP SYNERGY (LAKEBASE POSTGRES)
        </div>
        <span className="text-[8px] font-mono px-1.5 py-0.5 rounded border border-neon-orange/30 text-neon-orange/80">
          {data.source === "lakebase" ? "LAKEBASE" : "SQL FALLBACK"}
        </span>
      </div>

      {/* rETL flow indicator */}
      <div className="flex items-center gap-1 text-[8px] text-text-dim tracking-wider">
        <span className="text-neon-orange">DELTA</span>
        <span>→</span>
        <span className="text-neon-orange">SYNCED TABLE</span>
        <span>→</span>
        <span className="text-neon-orange">POSTGRES</span>
      </div>

      {data.synergy_cards.map((row, i) => (
        <div
          key={i}
          className="flex items-center justify-between bg-panel-light rounded px-3 py-1.5 border border-border"
        >
          <div className="flex items-center gap-2">
            <span className="text-neon-orange text-[10px] font-mono w-4">{i + 1}</span>
            <span className="text-xs font-bold tracking-wider">
              {row.card_b}
            </span>
          </div>
          <div className="flex items-center gap-3">
            {row.shared_tags && (
              <span className="text-[8px] text-text-dim">{row.shared_tags}</span>
            )}
            <span className="text-neon-orange font-mono text-xs font-bold tabular-nums">
              {(typeof row.synergy_score === "number"
                ? row.synergy_score
                : parseFloat(row.synergy_score)
              ).toFixed(3)}
            </span>
          </div>
        </div>
      ))}
    </div>
  );
}
