interface BatchData {
  synergy_cards: Array<{
    card_b: string;
    synergy_score: string;
    shared_tags: string;
    same_cluster: string;
  }>;
}

export default function BatchPanel({ data }: { data: BatchData | null }) {
  if (!data?.synergy_cards?.length) {
    return <div className="text-text-dim text-xs">No batch results</div>;
  }

  return (
    <div className="space-y-1">
      <div className="text-[9px] tracking-[0.2em] text-text-dim mb-2">
        TOP SYNERGY CARDS (PRE-COMPUTED)
      </div>
      {data.synergy_cards.map((row, i) => (
        <div
          key={i}
          className="flex items-center justify-between bg-panel-light rounded px-3 py-1.5 border border-border"
        >
          <div className="flex items-center gap-2">
            <span className="text-cyan text-[10px] font-mono w-4">{i + 1}</span>
            <span className="text-xs font-bold tracking-wider">
              {row.card_b}
            </span>
          </div>
          <div className="flex items-center gap-3">
            {row.shared_tags && (
              <span className="text-[8px] text-text-dim">{row.shared_tags}</span>
            )}
            <span className="text-cyan font-mono text-xs font-bold tabular-nums">
              {parseFloat(row.synergy_score).toFixed(3)}
            </span>
          </div>
        </div>
      ))}
    </div>
  );
}
