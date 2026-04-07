interface FeatureData {
  features: Record<string, any>;
}

const HIGHLIGHT_KEYS = ["card_name", "cost", "power", "ability_text", "tags", "archetype"];

export default function FeatureServingPanel({ data }: { data: FeatureData | null }) {
  if (!data?.features) {
    return <div className="text-text-dim text-xs">No features</div>;
  }

  const features = data.features;
  const highlighted = HIGHLIGHT_KEYS.filter((k) => k in features);
  const remaining = Object.keys(features).filter((k) => !HIGHLIGHT_KEYS.includes(k));

  return (
    <div className="space-y-3">
      <div className="text-[9px] tracking-[0.2em] text-text-dim">
        CARD FEATURE VECTOR
      </div>

      {/* Key stats */}
      <div className="grid grid-cols-3 gap-2">
        {features.cost !== undefined && (
          <div className="stat-box">
            <div className="stat-value text-neon-green">{features.cost}</div>
            <div className="stat-label">COST</div>
          </div>
        )}
        {features.power !== undefined && (
          <div className="stat-box">
            <div className="stat-value text-neon-green">{features.power}</div>
            <div className="stat-label">POWER</div>
          </div>
        )}
        {features.archetype && (
          <div className="stat-box">
            <div className="text-xs font-bold text-neon-green truncate">
              {features.archetype}
            </div>
            <div className="stat-label">ARCHETYPE</div>
          </div>
        )}
      </div>

      {/* Ability */}
      {features.ability_text && (
        <div className="bg-panel-light rounded p-2 border border-border">
          <div className="text-[8px] tracking-[0.2em] text-text-dim mb-1">ABILITY</div>
          <div className="text-[11px] font-mono text-white/80 leading-relaxed">
            {features.ability_text}
          </div>
        </div>
      )}

      {/* Tags */}
      {features.tags && (
        <div className="flex flex-wrap gap-1">
          {String(features.tags)
            .split(",")
            .map((tag: string) => tag.trim())
            .filter(Boolean)
            .map((tag: string) => (
              <span
                key={tag}
                className="px-2 py-0.5 text-[8px] border border-neon-green/30 rounded text-neon-green/80 tracking-wider"
              >
                {tag}
              </span>
            ))}
        </div>
      )}

      {/* Other features */}
      {remaining.length > 0 && (
        <div className="space-y-1">
          <div className="text-[8px] tracking-[0.2em] text-text-dim">OTHER FEATURES</div>
          {remaining.map((key) => (
            <div
              key={key}
              className="flex justify-between text-[10px] font-mono px-2 py-0.5 bg-panel-light rounded"
            >
              <span className="text-text-dim">{key}</span>
              <span className="text-white/70 tabular-nums">
                {typeof features[key] === "number"
                  ? features[key].toFixed(3)
                  : String(features[key])}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
