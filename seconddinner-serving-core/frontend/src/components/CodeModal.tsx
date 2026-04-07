import { useEffect } from "react";

interface CodeModalProps {
  open: boolean;
  onClose: () => void;
  title: string;
  code: string;
  color: string;
}

export default function CodeModal({ open, onClose, title, code, color }: CodeModalProps) {
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm"
      onClick={onClose}
    >
      <div
        className="bg-panel border border-border rounded-lg w-[90vw] max-w-2xl max-h-[80vh] flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-4 py-3 border-b border-border">
          <div className="flex items-center gap-2">
            <span className="text-[10px] font-mono" style={{ color }}>{"</>"}</span>
            <span className="text-xs font-black tracking-[0.15em]" style={{ color }}>
              {title}
            </span>
          </div>
          <button
            onClick={onClose}
            className="text-text-dim hover:text-white text-sm cursor-pointer px-2"
          >
            ✕
          </button>
        </div>
        <pre className="flex-1 overflow-auto p-4 text-[11px] font-mono text-white/80 leading-relaxed whitespace-pre">
          {code}
        </pre>
      </div>
    </div>
  );
}
