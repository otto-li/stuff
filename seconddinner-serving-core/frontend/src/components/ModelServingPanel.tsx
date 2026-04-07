import { useState } from "react";

interface ModelServingData {
  predictions: any;
}

const CENTROID_DISPLAY_COLS = ["cluster_id", "cluster_name", "cost", "power", "tag_count"];

export default function ModelServingPanel({ data }: { data: ModelServingData | null }) {
  const [showCentroids, setShowCentroids] = useState(false);

  if (!data?.predictions) {
    return <div className="text-text-dim text-xs">No cluster results</div>;
  }

  const pred = data.predictions;
  const assignment = pred.assignments?.[0] ?? pred[0] ?? pred;
  const centroids = pred.centroids ?? [];

  return (
    <div className="space-y-3">
      {/* Cluster assignment */}
      {assignment?.cluster_name && (
        <div className="text-center py-2">
          <div className="text-[9px] tracking-[0.2em] text-text-dim mb-1">
            ASSIGNED CLUSTER
          </div>
          <div className="text-xl font-black text-neon-red neon-text-red tracking-wider">
            {assignment.cluster_name}
          </div>
          {assignment.cluster_id !== undefined && (
            <div className="text-[9px] text-text-dim mt-1">
              CLUSTER {assignment.cluster_id}
              {assignment.distance !== undefined &&
                ` — DIST ${parseFloat(assignment.distance).toFixed(3)}`}
            </div>
          )}
        </div>
      )}

      {/* Cluster members */}
      {assignment?.cluster_members && (
        <div>
          <div className="text-[9px] tracking-[0.2em] text-text-dim mb-1">
            CLUSTER MEMBERS
          </div>
          <div className="flex flex-wrap gap-1">
            {assignment.cluster_members.map((name: string) => (
              <span
                key={name}
                className="px-2 py-0.5 text-[9px] border border-neon-red/30 rounded text-neon-red/80"
              >
                {name}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Centroids table — collapsible, compact */}
      {centroids.length > 0 && (
        <div>
          <button
            onClick={() => setShowCentroids(!showCentroids)}
            className="text-[9px] tracking-[0.2em] text-text-dim hover:text-white transition-colors cursor-pointer flex items-center gap-1"
          >
            <span className="text-[8px]">{showCentroids ? "▼" : "▶"}</span>
            CENTROIDS ({centroids.length})
          </button>
          {showCentroids && (
            <div className="overflow-x-auto mt-1">
              <table className="w-full text-[9px] font-mono">
                <thead>
                  <tr className="border-b border-border">
                    {CENTROID_DISPLAY_COLS.map((col) => (
                      <th
                        key={col}
                        className="px-1.5 py-0.5 text-left text-text-dim tracking-wider font-bold"
                      >
                        {col.replace("cluster_", "").replace("tag_count", "tags")}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {centroids.map((row: any, i: number) => (
                    <tr
                      key={i}
                      className={`border-b border-border/30 ${
                        row.cluster_id === assignment?.cluster_id
                          ? "bg-neon-red/10 text-neon-red"
                          : "text-white/50"
                      }`}
                    >
                      {CENTROID_DISPLAY_COLS.map((col) => (
                        <td key={col} className="px-1.5 py-0.5 tabular-nums">
                          {typeof row[col] === "number"
                            ? Number.isInteger(row[col]) ? row[col] : row[col].toFixed(1)
                            : row[col]}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
