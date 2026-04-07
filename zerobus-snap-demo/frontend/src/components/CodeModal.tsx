interface Props {
  onClose: () => void;
}

const CODE = `# ─────────────────────────────────────────────────────────
#  Zerobus Ingest SDK  •  Marvel Snap Producer
#  pip install databricks-zerobus-ingest-sdk
# ─────────────────────────────────────────────────────────

from zerobus.sdk.aio import ZerobusSdk
from zerobus.sdk.shared import (
    AckCallback,
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
)
import asyncio, json, uuid, random
from datetime import datetime, timezone


class SnapAckCallback(AckCallback):
    """Called from Rust WAL thread when each record is durably committed."""

    def __init__(self, stats):
        self.stats = stats

    def on_ack(self, sequence_num: int) -> None:
        # WAL has committed — safe to remove from in-flight buffer
        self.stats["events_acked"] += 1
        self.stats["in_flight"] = max(0, self.stats["in_flight"] - 1)


async def produce(endpoint: str, client_id: str, client_secret: str):
    sdk = ZerobusSdk(
        server_endpoint=endpoint,
        client_id=client_id,
        client_secret=client_secret,
    )

    stats = {"events_sent": 0, "events_acked": 0, "in_flight": 0}
    callback = SnapAckCallback(stats)

    options = StreamConfigurationOptions(
        table_properties=TableProperties(
            table_name="main.default.snap_game_events"
        )
    )

    # Open a persistent stream — reconnects automatically on failure
    stream = await sdk.open_stream(options, callback)

    seq = 0
    while True:
        seq += 1
        event = {
            "event_id":   str(uuid.uuid4()),
            "event_type": random.choice(["match_started", "card_played",
                                         "snap_triggered", "match_ended"]),
            "player_id":  f"player_{random.randint(1, 50):04d}",
            "match_id":   f"match_{random.randint(1, 200):06d}",
            "card_name":  random.choice(["Iron Man", "Thor", "Galactus",
                                          "Doctor Doom", "Silver Surfer"]),
            "location":   random.randint(1, 3),
            "snap_cubes": random.choice([1, 2, 4, 8]),
            "result":     random.choice(["win", "loss", "retreat"]),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "sequence_num": seq,
        }

        # ingest() writes to the WAL — returns BEFORE Delta materialization
        # on_ack() fires (from Rust thread) once WAL commit is durable
        record = RecordType(payload=json.dumps(event).encode())
        await stream.ingest(record)
        stats["events_sent"] += 1
        stats["in_flight"] += 1

        await asyncio.sleep(0.2)  # 5 events/sec


# ─── Durability guarantee ─────────────────────────────────
#
#  1. ingest()    → write to WAL (fast, local)
#  2. on_ack()    → WAL commit confirmed (durable)
#  3. Background  → Zerobus worker materializes WAL → Delta
#
#  Hard crash between steps 1 and 3?
#  • Acked records (step 2) are in WAL — guaranteed to reach Delta
#  • In-flight records (between 1 and 2) are re-sent on reconnect
#  • SELECT COUNT(*) after crash will equal events_acked before crash
#
# ─── At-least-once delivery ───────────────────────────────
#
#  After crash + reconnect, un-acked records are re-sent automatically.
#  Dedup in SQL:
#    SELECT DISTINCT event_id FROM main.default.snap_game_events
#
#  Or use sequence_num for gap detection:
#    SELECT sequence_num FROM snap_game_events ORDER BY sequence_num
# ─────────────────────────────────────────────────────────`;

export default function CodeModal({ onClose }: Props) {
  return (
    <div
      className="fixed inset-0 bg-void/90 backdrop-blur-sm z-50 flex items-center justify-center p-4"
      onClick={onClose}
    >
      <div
        className="bg-panel border border-border rounded-xl max-w-3xl w-full max-h-[90vh] overflow-hidden flex flex-col shadow-neon-cyan"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-border">
          <div>
            <h2 className="text-sm font-black tracking-[0.2em] text-cyan neon-text-cyan">
              ZEROBUS SDK — PRODUCER PATTERN
            </h2>
            <p className="text-[9px] text-text-dim tracking-widest mt-0.5">
              pip install databricks-zerobus-ingest-sdk
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-muted hover:text-white transition-colors text-lg"
          >
            ✕
          </button>
        </div>

        {/* Code */}
        <div className="flex-1 overflow-y-auto p-6">
          <pre className="text-[11px] font-mono text-text-dim leading-relaxed whitespace-pre-wrap">
            {CODE.split("\n").map((line, i) => {
              // Syntax highlighting via className
              const isComment = line.trim().startsWith("#");
              const isKeyword =
                /^(from|import|class|def|async|await|return|while|if|for)\b/.test(
                  line.trim()
                );
              const isString = line.includes('"') || line.includes("'");

              return (
                <span
                  key={i}
                  className={
                    isComment
                      ? "text-muted block"
                      : isKeyword
                      ? "text-neon-purple block"
                      : "text-text-dim block"
                  }
                >
                  {line || " "}
                </span>
              );
            })}
          </pre>
        </div>

        {/* Key facts */}
        <div className="px-6 py-4 border-t border-border bg-panel-light">
          <div className="grid grid-cols-3 gap-4 text-[9px]">
            <div>
              <p className="text-cyan tracking-widest font-black mb-1">WAL ACKS</p>
              <p className="text-text-dim">on_ack() fires from Rust thread when record is durably in WAL — before Delta materialization</p>
            </div>
            <div>
              <p className="text-gold tracking-widest font-black mb-1">NO BROKER</p>
              <p className="text-text-dim">Zerobus writes directly to Unity Catalog Delta — eliminates Kafka/Kinesis/Pub-Sub entirely</p>
            </div>
            <div>
              <p className="text-neon-green tracking-widest font-black mb-1">AUTO-RECONNECT</p>
              <p className="text-text-dim">SDK handles TCP reconnect and at-least-once replay automatically — no application logic needed</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
