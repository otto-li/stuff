import { useState, useCallback } from "react";
import { SAMPLE_CARDS, SERVING_CONFIGS, ServingType } from "./types";
import CardPicker from "./components/CardPicker";
import ServingBadge from "./components/ServingBadge";
import CodeModal from "./components/CodeModal";
import BatchPanel from "./components/BatchPanel";
import ModelServingPanel from "./components/ModelServingPanel";
import FeatureServingPanel from "./components/FeatureServingPanel";
import VectorSearchPanel from "./components/VectorSearchPanel";
import FoundationModelPanel from "./components/FoundationModelPanel";
import RetlPanel from "./components/RetlPanel";
import { CODE_EXAMPLES } from "./codeExamples";

interface PanelState {
  loading: boolean;
  data: any;
  error: string | null;
  latency_ms: number | null;
}

const EMPTY: PanelState = { loading: false, data: null, error: null, latency_ms: null };

export default function App() {
  const [selectedCard, setSelectedCard] = useState<string | null>(null);
  const [panels, setPanels] = useState<Record<ServingType, PanelState>>({
    batch: EMPTY,
    model_serving: EMPTY,
    feature_serving: EMPTY,
    vector_search: EMPTY,
    foundation_model: EMPTY,
    retl: EMPTY,
  });

  const fetchPanel = useCallback(
    async (type: ServingType, url: string) => {
      setPanels((prev) => ({
        ...prev,
        [type]: { loading: true, data: null, error: null, latency_ms: null },
      }));
      try {
        const resp = await fetch(url);
        if (!resp.ok) {
          const err = await resp.text();
          throw new Error(err);
        }
        const data = await resp.json();
        setPanels((prev) => ({
          ...prev,
          [type]: {
            loading: false,
            data,
            error: null,
            latency_ms: data.latency_ms ?? null,
          },
        }));
      } catch (e: any) {
        setPanels((prev) => ({
          ...prev,
          [type]: { loading: false, data: null, error: e.message, latency_ms: null },
        }));
      }
    },
    []
  );

  const handleSelectCard = useCallback(
    (card: string) => {
      setSelectedCard(card);
      const encoded = encodeURIComponent(card);
      // Fire all 6 panels simultaneously
      fetchPanel("batch", `/api/batch/${encoded}`);
      fetchPanel("model_serving", `/api/cluster/${encoded}`);
      fetchPanel("feature_serving", `/api/features/${encoded}`);
      fetchPanel("vector_search", `/api/similar/${encoded}`);
      fetchPanel("foundation_model", `/api/explain/${encoded}`);
      fetchPanel("retl", `/api/retl/${encoded}`);
    },
    [fetchPanel]
  );

  return (
    <div className="min-h-screen bg-void text-white font-orbitron">
      {/* Header */}
      <header className="border-b border-border px-6 py-5">
        <div className="max-w-[1600px] mx-auto flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-black tracking-[0.2em] neon-text-cyan">
              SECONDDINNER SERVING CORE
            </h1>
            <p className="text-[10px] tracking-[0.3em] text-text-dim mt-1">
              MARVEL SNAP CARD SYNERGY — DATABRICKS SERVING MODALITIES
            </p>
          </div>
          <div className="flex items-center gap-3">
            {selectedCard && (
              <span className="text-xs text-text-dim tracking-wider">
                ACTIVE:{" "}
                <span className="text-cyan font-bold">{selectedCard.toUpperCase()}</span>
              </span>
            )}
          </div>
        </div>
      </header>

      <main className="max-w-[1600px] mx-auto px-6 py-6 space-y-6">
        {/* Card Picker */}
        <CardPicker
          cards={SAMPLE_CARDS}
          selected={selectedCard}
          onSelect={handleSelectCard}
        />

        {!selectedCard && (
          <div className="text-center py-20 text-text-dim text-sm tracking-widest">
            SELECT A CARD ABOVE TO QUERY ALL SIX SERVING MODALITIES
          </div>
        )}

        {selectedCard && (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {/* Panel 1: Batch */}
            <PanelWrapper type="batch" state={panels.batch}>
              <BatchPanel data={panels.batch.data} />
            </PanelWrapper>

            {/* Panel 2: Model Serving */}
            <PanelWrapper type="model_serving" state={panels.model_serving}>
              <ModelServingPanel data={panels.model_serving.data} />
            </PanelWrapper>

            {/* Panel 3: Feature Serving */}
            <PanelWrapper type="feature_serving" state={panels.feature_serving}>
              <FeatureServingPanel data={panels.feature_serving.data} />
            </PanelWrapper>

            {/* Panel 4: Vector Search */}
            <PanelWrapper type="vector_search" state={panels.vector_search}>
              <VectorSearchPanel data={panels.vector_search.data} />
            </PanelWrapper>

            {/* Panel 5: Foundation Model */}
            <PanelWrapper type="foundation_model" state={panels.foundation_model}>
              <FoundationModelPanel data={panels.foundation_model.data} />
            </PanelWrapper>

            {/* Panel 6: rETL / Lakebase */}
            <PanelWrapper type="retl" state={panels.retl}>
              <RetlPanel data={panels.retl.data} />
            </PanelWrapper>
          </div>
        )}
      </main>
    </div>
  );
}

function PanelWrapper({
  type,
  state,
  children,
}: {
  type: ServingType;
  state: PanelState;
  children: React.ReactNode;
}) {
  const [showCode, setShowCode] = useState(false);
  const config = SERVING_CONFIGS[type];

  return (
    <div className="panel animate-slide-in">
      <div className="flex items-center justify-between mb-3">
        <ServingBadge type={type} />
        <div className="flex items-center gap-2">
          {state.latency_ms !== null && (
            <span className="text-[10px] font-mono text-text-dim">
              {state.latency_ms}ms
            </span>
          )}
          <button
            onClick={() => setShowCode(true)}
            className="text-[9px] font-mono px-1.5 py-0.5 border rounded cursor-pointer transition-all"
            style={{
              borderColor: config.color + "50",
              color: config.color,
            }}
            title="View example code"
          >
            {"</>"}
          </button>
        </div>
      </div>

      {state.loading && (
        <div className="space-y-2">
          <div className="h-4 loading-shimmer rounded" />
          <div className="h-4 loading-shimmer rounded w-3/4" />
          <div className="h-4 loading-shimmer rounded w-1/2" />
        </div>
      )}

      {state.error && (
        <div className="text-neon-red text-xs font-mono break-all">
          {state.error}
        </div>
      )}

      {!state.loading && !state.error && state.data && children}

      <CodeModal
        open={showCode}
        onClose={() => setShowCode(false)}
        title={config.label}
        code={CODE_EXAMPLES[type]}
        color={config.color}
      />
    </div>
  );
}
