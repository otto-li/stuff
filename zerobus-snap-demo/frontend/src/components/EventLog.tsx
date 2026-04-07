import { useRef, useEffect } from "react";
import { EventLogEntry } from "../types";

interface Props {
  events: EventLogEntry[];
}

const STATUS_COLOR: Record<string, string> = {
  sent: "text-text-dim",
  acked: "text-neon-green",
  rejected: "text-neon-red",
  resent: "text-neon-yellow",
  failed: "text-neon-red",
};

const TYPE_COLOR: Record<string, string> = {
  match_started: "text-cyan",
  card_played: "text-gold",
  snap_triggered: "text-neon-purple",
  match_ended: "text-neon-green",
};

const TYPE_ICON: Record<string, string> = {
  match_started: "◈",
  card_played: "▲",
  snap_triggered: "✦",
  match_ended: "◉",
};

export default function EventLog({ events }: Props) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const prevTopRef = useRef<string>("");

  useEffect(() => {
    if (events.length > 0 && events[0]?.event_id !== prevTopRef.current) {
      prevTopRef.current = events[0]?.event_id ?? "";
      if (scrollRef.current) {
        scrollRef.current.scrollTop = 0;
      }
    }
  }, [events]);

  return (
    <div className="panel h-full flex flex-col">
      <div className="flex items-center justify-between mb-3">
        <p className="panel-title mb-0">Live Event Stream</p>
        <span className="text-[9px] text-text-dim tracking-widest">
          LAST {events.length} EVENTS
        </span>
      </div>

      {/* Column headers */}
      <div className="grid grid-cols-[20px_90px_70px_90px_40px_72px_46px] gap-1.5 text-[8px] text-muted tracking-widest pb-1 border-b border-border mb-1">
        <span></span>
        <span>TYPE</span>
        <span>PLAYER</span>
        <span>CARD</span>
        <span>SEQ</span>
        <span>EVENT ID</span>
        <span className="text-right">STATUS</span>
      </div>

      <div
        ref={scrollRef}
        className="flex-1 overflow-y-auto space-y-0.5 min-h-0"
        style={{ maxHeight: "460px" }}
      >
        {events.length === 0 ? (
          <div className="flex items-center justify-center h-32 text-[10px] text-muted tracking-widest">
            Awaiting events...
          </div>
        ) : (
          events.map((ev, i) => (
            <EventRow key={`${ev.event_id}-${ev.sequence_num}`} ev={ev} isNew={i === 0} />
          ))
        )}
      </div>
    </div>
  );
}

function EventRow({ ev, isNew }: { ev: EventLogEntry; isNew: boolean }) {
  const typeColor = TYPE_COLOR[ev.event_type] ?? "text-text-dim";
  const statusColor = STATUS_COLOR[ev.status] ?? "text-text-dim";
  // Show first 8 chars of UUID for compact display; full UUID on title tooltip
  const shortId = ev.event_id ? ev.event_id.slice(0, 8) : "--------";

  return (
    <div
      title={ev.event_id}
      className={`grid grid-cols-[20px_90px_70px_90px_40px_72px_46px] gap-1.5 items-center py-0.5 px-1 rounded text-[9px] font-mono hover:bg-panel-light transition-colors ${
        isNew ? "animate-slide-in" : ""
      }`}
    >
      <span className={`text-center ${typeColor}`}>
        {TYPE_ICON[ev.event_type] ?? "•"}
      </span>
      <span className={`truncate font-black tracking-wide ${typeColor}`}>
        {ev.event_type.replace("_", " ").toUpperCase().slice(0, 12)}
      </span>
      <span className="text-text-dim truncate">{ev.player_id}</span>
      <span className="text-white truncate">{ev.card_name}</span>
      <span className="text-muted tabular-nums text-right">#{ev.sequence_num}</span>
      <span className="text-muted/60 tabular-nums text-[8px]">{shortId}…</span>
      <span className={`${statusColor} uppercase text-[8px] tracking-widest text-right`}>
        {ev.status}
      </span>
    </div>
  );
}
