export type ProducerStateValue = "STOPPED" | "RUNNING" | "KILLED" | "RECONNECTING";

export interface EventLogEntry {
  event_id: string;
  event_type: string;
  player_id: string;
  card_name: string;
  sequence_num: number;
  status: "sent" | "acked" | "rejected" | "resent" | "failed";
  timestamp: string;
  color: string;
}

export interface ProducerStats {
  state: ProducerStateValue;
  events_sent: number;
  events_acked: number;
  events_in_flight: number;
  events_failed: number;
  events_per_sec: number;
  acked_at_kill: number;
  unacked_at_kill: number;
  events_resent: number;
  rejection_count: number;
  demo_mode: boolean;
  event_log: EventLogEntry[];
  sequence_num: number;
  rate: number;
  delta_count: number;
  delta_by_type: Record<string, number>;
  last_error: string;
}

export const DEFAULT_STATS: ProducerStats = {
  state: "STOPPED",
  events_sent: 0,
  events_acked: 0,
  events_in_flight: 0,
  events_failed: 0,
  events_per_sec: 0,
  acked_at_kill: 0,
  unacked_at_kill: 0,
  events_resent: 0,
  rejection_count: 0,
  demo_mode: false,
  event_log: [],
  sequence_num: 0,
  rate: 5,
  delta_count: 0,
  delta_by_type: {},
  last_error: "",
};
