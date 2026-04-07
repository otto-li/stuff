interface VectorData {
  similar_cards: Array<{
    card_name: string;
    ability_text?: string;
    score?: number;
  }>;
}

export default function VectorSearchPanel({ data }: { data: VectorData | null }) {
  if (!data?.similar_cards?.length) {
    return <div className="text-text-dim text-xs">No similar cards found</div>;
  }

  return (
    <div className="space-y-1">
      <div className="text-[9px] tracking-[0.2em] text-text-dim mb-2">
        SEMANTICALLY SIMILAR (BY ABILITY EMBEDDING)
      </div>
      {data.similar_cards.map((card, i) => (
        <div
          key={i}
          className="bg-panel-light rounded px-3 py-2 border border-border space-y-1"
        >
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <span className="text-neon-purple text-[10px] font-mono w-4">
                {i + 1}
              </span>
              <span className="text-xs font-bold tracking-wider">
                {card.card_name}
              </span>
            </div>
            {card.score !== undefined && (
              <span className="text-neon-purple font-mono text-[10px] tabular-nums">
                {(typeof card.score === "number" ? card.score : parseFloat(card.score)).toFixed(
                  3
                )}
              </span>
            )}
          </div>
          {card.ability_text && (
            <div className="text-[9px] font-mono text-white/50 leading-snug pl-6">
              {card.ability_text}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
