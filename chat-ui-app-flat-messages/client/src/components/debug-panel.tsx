import { useState } from 'react';

interface DebugPanelProps {
  requestPayload: unknown;
  llmPayload: unknown;
}

type Tab = 'request' | 'llm';

export function DebugPanel({ requestPayload, llmPayload }: DebugPanelProps) {
  const [open, setOpen] = useState(false);
  const [tab, setTab] = useState<Tab>('request');

  return (
    <div className="fixed bottom-4 right-4 z-50 flex flex-col items-end gap-2">
      {open && (
        <div className="w-96 max-h-[480px] flex flex-col rounded-lg border border-gray-700 bg-gray-900/95 shadow-xl backdrop-blur text-gray-100">
          <div className="flex shrink-0 border-b border-gray-700">
            <button
              type="button"
              onClick={() => setTab('request')}
              className={`px-3 py-2 text-xs font-medium transition-colors ${
                tab === 'request'
                  ? 'text-blue-400 border-b-2 border-blue-400'
                  : 'text-gray-400 hover:text-gray-200'
              }`}
            >
              → API
            </button>
            <button
              type="button"
              onClick={() => setTab('llm')}
              className={`px-3 py-2 text-xs font-medium transition-colors ${
                tab === 'llm'
                  ? 'text-blue-400 border-b-2 border-blue-400'
                  : 'text-gray-400 hover:text-gray-200'
              }`}
            >
              ← LLM
            </button>
            <span className="ml-auto px-3 py-2 text-xs text-gray-500">
              {tab === 'request' ? 'client payload' : 'LLM messages array'}
            </span>
          </div>
          <div className="overflow-auto p-3">
            <pre className="text-xs font-mono whitespace-pre-wrap break-words leading-relaxed">
              {JSON.stringify(
                tab === 'request' ? requestPayload : llmPayload,
                null,
                2,
              ) ?? 'null'}
            </pre>
          </div>
        </div>
      )}
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="flex h-8 w-8 items-center justify-center rounded-full bg-gray-800 text-gray-300 text-xs shadow-lg hover:bg-gray-700 hover:text-white border border-gray-600 transition-colors"
        title="Toggle debug panel"
      >
        {'</>'}
      </button>
    </div>
  );
}
