import { useCallback, useRef, useState } from "react";
import { Upload } from "lucide-react";

interface ImageUploadProps {
  onIdentify: (file: File) => void;
  loading: boolean;
}

export default function ImageUpload({ onIdentify, loading }: ImageUploadProps) {
  const [preview, setPreview] = useState<string | null>(null);
  const [dragging, setDragging] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const handleFile = useCallback(
    (file: File) => {
      if (!file.type.startsWith("image/")) return;
      const reader = new FileReader();
      reader.onload = (e) => setPreview(e.target?.result as string);
      reader.readAsDataURL(file);
      onIdentify(file);
    },
    [onIdentify]
  );

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragging(false);
      const file = e.dataTransfer.files[0];
      if (file) handleFile(file);
    },
    [handleFile]
  );

  return (
    <div className="w-full max-w-2xl mx-auto">
      <div
        className={`relative rounded-2xl border-2 border-dashed transition-all cursor-pointer overflow-hidden
          ${dragging ? "border-red-400 bg-red-900/10" : "border-gray-700 hover:border-gray-500 bg-gray-900/50"}`}
        style={{ minHeight: 280 }}
        onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
        onDragLeave={() => setDragging(false)}
        onDrop={onDrop}
        onClick={() => !loading && inputRef.current?.click()}
      >
        <input
          ref={inputRef}
          type="file"
          accept="image/*"
          className="hidden"
          onChange={(e) => { const f = e.target.files?.[0]; if (f) handleFile(f); }}
        />

        {preview ? (
          <div className="relative w-full h-full" style={{ minHeight: 280 }}>
            <img
              src={preview}
              alt="Uploaded Pokémon"
              className="w-full h-full object-contain"
              style={{ maxHeight: 320 }}
            />
            {/* Scan overlay when loading */}
            {loading && (
              <div className="absolute inset-0 flex flex-col items-center justify-center bg-black/60">
                <div
                  className="w-16 h-16 rounded-full border-4 border-t-red-500 border-r-white border-b-red-500 border-l-white pokeball-spin mb-4"
                />
                <span className="font-pixel text-[10px] text-green-400 animate-pulse">
                  SCANNING...
                </span>
                <span className="font-pixel text-[8px] text-gray-400 mt-1">
                  Claude Vision is analyzing
                </span>
              </div>
            )}
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center h-full py-16 gap-4">
            {/* Pokéball icon */}
            <div className="relative w-20 h-20">
              <div className="w-20 h-20 rounded-full overflow-hidden border-4 border-gray-600 shadow-lg">
                <div className="w-full h-10 bg-red-600" />
                <div className="w-full h-10 bg-white" />
              </div>
              <div className="absolute inset-0 flex items-center justify-center">
                <div className="w-6 h-6 rounded-full bg-gray-900 border-4 border-gray-600 z-10" />
              </div>
              <div className="absolute left-0 right-0 top-[calc(50%-2px)] h-1 bg-gray-600" />
            </div>
            <div className="text-center space-y-2">
              <p className="font-pixel text-[11px] text-gray-300 flex items-center gap-2">
                <Upload size={14} className="text-red-400" />
                Upload a Pokémon image
              </p>
              <p className="text-[9px] text-gray-500 font-mono">
                drag & drop or click to browse
              </p>
              <p className="text-[8px] text-gray-600 font-mono">
                PNG, JPG, GIF, WEBP supported
              </p>
            </div>
          </div>
        )}
      </div>

      {/* Change image button when preview exists */}
      {preview && !loading && (
        <button
          onClick={() => inputRef.current?.click()}
          className="mt-3 w-full py-2 text-[9px] font-pixel text-gray-400 hover:text-white border border-gray-700 hover:border-gray-500 rounded-lg transition-colors"
        >
          ↑ UPLOAD DIFFERENT IMAGE
        </button>
      )}
    </div>
  );
}
