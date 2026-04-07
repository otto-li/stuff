import { SERVING_CONFIGS, ServingType } from "../types";
import {
  Database,
  Zap,
  Table2,
  Search,
  Brain,
  RefreshCw,
} from "lucide-react";

const ICONS: Record<ServingType, typeof Database> = {
  batch: Database,
  model_serving: Zap,
  feature_serving: Table2,
  vector_search: Search,
  foundation_model: Brain,
  retl: RefreshCw,
};

export default function ServingBadge({ type }: { type: ServingType }) {
  const config = SERVING_CONFIGS[type];
  const Icon = ICONS[type];

  return (
    <div className="flex items-center gap-2">
      <div
        className={`flex items-center gap-1.5 px-2 py-1 rounded border ${config.borderClass} ${config.textClass}`}
        style={{ borderWidth: 1 }}
      >
        <Icon size={12} />
        <span className="text-[9px] font-black tracking-[0.15em]">{config.label}</span>
      </div>
      <span className="text-[8px] tracking-[0.15em] text-text-dim">{config.sublabel}</span>
    </div>
  );
}
