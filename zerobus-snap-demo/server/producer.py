"""
Zerobus producer — Marvel Snap game events.

State machine:  STOPPED → RUNNING → KILLED → RECONNECTING → RUNNING

Uses the real databricks-zerobus-ingest-sdk when ZEROBUS_HOST is configured
and the service is reachable.  Automatically falls back to demo mode if the
SDK raises ZerobusException/Unimplemented on connect.

SDK API (v1.0.0):
    sdk = ZerobusSdk(host=..., unity_catalog_url=...)
    stream = await sdk.create_stream(
        client_id, client_secret, table_properties, options, headers_provider
    )
    await stream.ingest_record_offset(payload_bytes)  # returns offset
    stream.ingest_record_nowait(payload_bytes)        # fire-and-forget
    await stream.flush()
    await stream.close()
    await stream.get_unacked_records()               # at-least-once replay
"""

import asyncio
import json
import socket
import uuid
import time
import random
import urllib.request
import urllib.parse
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

# Real SDK (optional — falls back to demo mode if unavailable or Unimplemented)
ZEROBUS_SDK_AVAILABLE = False
ZEROBUS_IMPORT_ERROR = ""
try:
    from zerobus.sdk.aio import (  # type: ignore
        ZerobusSdk, ZerobusStream,
        HeadersProvider,
        TableProperties, StreamConfigurationOptions, RecordType,
    )
    from zerobus._zerobus_core import AckCallback  # type: ignore  # Rust base class

    ZEROBUS_SDK_AVAILABLE = True
except Exception as _e:
    ZEROBUS_IMPORT_ERROR = str(_e)

# ── Protobuf schema for game_events ───────────────────────────────────────────
# Self-contained descriptor (no external dependencies — Zerobus requirement).
# Timestamps are INT64 unix microseconds (Delta table uses BIGINT columns).
from google.protobuf import descriptor_pool as _dp, message_factory as _mf
from google.protobuf.descriptor_pb2 import FileDescriptorProto as _FDP, FieldDescriptorProto as _FldDP

_file_proto = _FDP()
_file_proto.name = "game_events.proto"
_file_proto.syntax = "proto3"
_msg = _file_proto.message_type.add()
_msg.name = "GameEvent"
for _name, _num, _type in [
    ("event_id",     1,  _FldDP.TYPE_STRING),
    ("event_type",   2,  _FldDP.TYPE_STRING),
    ("player_id",    3,  _FldDP.TYPE_STRING),
    ("match_id",     4,  _FldDP.TYPE_STRING),
    ("card_name",    5,  _FldDP.TYPE_STRING),
    ("location",     6,  _FldDP.TYPE_INT32),
    ("snap_cubes",   7,  _FldDP.TYPE_INT32),
    ("result",       8,  _FldDP.TYPE_STRING),
    ("produced_at",  9,  _FldDP.TYPE_INT64),   # unix microseconds
    ("ingested_at",  10, _FldDP.TYPE_INT64),   # unix microseconds
    ("host",         11, _FldDP.TYPE_STRING),
    ("sequence_num", 12, _FldDP.TYPE_INT64),
]:
    _f = _msg.field.add()
    _f.name, _f.number, _f.type = _name, _num, _type
    _f.label = _FldDP.LABEL_OPTIONAL
_dp.Default().Add(_file_proto)
GameEventProto = _mf.GetMessageClass(_dp.Default().FindMessageTypeByName("GameEvent"))
GAME_EVENT_DESCRIPTOR_BYTES = _file_proto.SerializeToString()


def _make_proto_payload(event: dict) -> bytes:
    """Serialize a game event dict as a protobuf GameEvent."""
    from datetime import datetime, timezone

    def _to_us(iso_str: str) -> int:
        try:
            dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1_000_000)
        except Exception:
            return int(datetime.now(timezone.utc).timestamp() * 1_000_000)

    return GameEventProto(
        event_id=event.get("event_id", ""),
        event_type=event.get("event_type", ""),
        player_id=event.get("player_id", ""),
        match_id=event.get("match_id", ""),
        card_name=event.get("card_name", ""),
        location=int(event.get("location", 0)),
        snap_cubes=int(event.get("snap_cubes", 0)),
        result=event.get("result", ""),
        produced_at=_to_us(event.get("produced_at", "")),
        ingested_at=_to_us(event.get("ingested_at", "")),
        host=event.get("host", ""),
        sequence_num=int(event.get("sequence_num", 0)),
    ).SerializeToString()

import os
from .config import get_zerobus_config, get_workspace_client

# Hostname of this producer instance — included in every event
_HOSTNAME = socket.gethostname()


# ── Constants ──────────────────────────────────────────────────────────────────

class ProducerState(str, Enum):
    STOPPED = "STOPPED"
    RUNNING = "RUNNING"
    KILLED = "KILLED"
    RECONNECTING = "RECONNECTING"


CARD_NAMES = [
    "Iron Man", "Wolverine", "Spider-Man", "Thor", "Hulk",
    "Doctor Doom", "Galactus", "Silver Surfer", "Magneto", "Black Panther",
    "Deadpool", "Thanos", "Captain America", "Venom", "Ghost Rider",
    "Storm", "Electro", "Onslaught", "Hela", "Knull",
    "Wong", "Spectrum", "Mystique", "Loki", "Moon Knight",
]

EVENT_TYPES = ["match_started", "card_played", "snap_triggered", "match_ended"]

EVENT_COLORS: Dict[str, str] = {
    "match_started": "cyan",
    "card_played": "gold",
    "snap_triggered": "purple",
    "match_ended": "green",
}

PLAYERS = [f"player_{i:04d}" for i in range(1, 51)]
MATCHES = [f"match_{i:06d}" for i in range(1, 201)]


# ── HeadersProvider (uses Databricks SDK token — works in App + local) ─────────

def _fetch_zerobus_token(client_id: str, client_secret: str, workspace_host: str, table_name: str) -> str:
    """
    Get an OAuth token scoped to the Zerobus DirectWrite API.
    Uses resource=api://databricks/workspaces/{id}/zerobusDirectWriteApi and
    authorization_details with UC catalog/schema/table privileges.
    """
    host = workspace_host.rstrip("/")
    if not host.startswith("http"):
        host = f"https://{host}"

    # Extract workspace org ID from ZEROBUS_HOST (e.g. https://2198414303818321.zerobus....)
    zerobus_host = os.environ.get("ZEROBUS_HOST", "")
    workspace_id = zerobus_host.split("://")[-1].split(".")[0] if zerobus_host else ""

    # Parse table name → catalog / schema / table
    parts = table_name.split(".")
    catalog = parts[0] if len(parts) > 0 else ""
    schema_full = f"{parts[0]}.{parts[1]}" if len(parts) > 1 else ""

    resource = f"api://databricks/workspaces/{workspace_id}/zerobusDirectWriteApi"
    auth_details = json.dumps([
        {"type": "unity_catalog_permission", "securable_type": "CATALOG",
         "securable_name": catalog, "operation": "USE_CATALOG"},
        {"type": "unity_catalog_permission", "securable_type": "SCHEMA",
         "securable_name": schema_full, "operation": "USE_SCHEMA"},
        {"type": "unity_catalog_permission", "securable_type": "TABLE",
         "securable_name": table_name, "operation": "MODIFY"},
    ])

    url = f"{host}/oidc/v1/token"
    data = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "resource": resource,
        "authorization_details": auth_details,
    }).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())["access_token"]


if ZEROBUS_SDK_AVAILABLE:
    class _DatabricksHeadersProvider(HeadersProvider):  # type: ignore
        """
        Provides fresh Bearer token for each gRPC request.

        Priority:
        1. ZEROBUS_CLIENT_ID/SECRET env vars — fetch M2M token for Zerobus SP
        2. DATABRICKS_TOKEN env var — injected by Databricks Apps runtime
        3. Workspace client token — local dev fallback
        """

        def get_headers(self):
            zb_client_id = os.environ.get("ZEROBUS_CLIENT_ID", "")
            zb_client_secret = os.environ.get("ZEROBUS_CLIENT_SECRET", "")
            db_host = os.environ.get("DATABRICKS_HOST", "")
            table_name = os.environ.get("ZEROBUS_TABLE_NAME", "")
            if zb_client_id and zb_client_secret and db_host and table_name:
                try:
                    token = _fetch_zerobus_token(zb_client_id, zb_client_secret, db_host, table_name)
                    return [("authorization", f"Bearer {token}")]
                except Exception as e:
                    print(f"HeadersProvider Zerobus token error: {e}")

            token = os.environ.get("DATABRICKS_TOKEN", "")
            if not token:
                try:
                    client = get_workspace_client()
                    token = client.config.authenticate().get("Authorization", "").replace("Bearer ", "")
                except Exception as e:
                    print(f"HeadersProvider error: {e}")
            return [("authorization", f"Bearer {token}")] if token else []


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class ProducerStats:
    state: ProducerState = ProducerState.STOPPED
    events_sent: int = 0
    events_acked: int = 0
    events_in_flight: int = 0
    events_failed: int = 0
    events_per_sec: float = 0.0
    acked_at_kill: int = 0
    unacked_at_kill: int = 0
    events_resent: int = 0
    rejection_count: int = 0
    demo_mode: bool = True
    event_log: List[Dict[str, Any]] = field(default_factory=list)
    sequence_num: int = 0
    rate: int = 5
    delta_count: int = 0
    delta_by_type: Dict[str, int] = field(default_factory=dict)
    last_error: str = ""

    def add_event_log(self, event: dict, status: str = "sent") -> None:
        entry = {
            "event_id": event["event_id"][:8],
            "event_type": event["event_type"],
            "player_id": event["player_id"],
            "card_name": event["card_name"],
            "sequence_num": event["sequence_num"],
            "status": status,
            "timestamp": event.get("produced_at", ""),
            "color": EVENT_COLORS.get(event["event_type"], "white"),
        }
        self.event_log.append(entry)
        if len(self.event_log) > 50:
            self.event_log = self.event_log[-50:]

    def to_dict(self) -> dict:
        return {
            "state": self.state.value,
            "events_sent": self.events_sent,
            "events_acked": self.events_acked,
            "events_in_flight": self.events_in_flight,
            "events_failed": self.events_failed,
            "events_per_sec": round(self.events_per_sec, 1),
            "acked_at_kill": self.acked_at_kill,
            "unacked_at_kill": self.unacked_at_kill,
            "events_resent": self.events_resent,
            "rejection_count": self.rejection_count,
            "demo_mode": self.demo_mode,
            "event_log": list(reversed(self.event_log[-50:])),
            "sequence_num": self.sequence_num,
            "rate": self.rate,
            "delta_count": self.delta_count,
            "delta_by_type": dict(self.delta_by_type),
            "last_error": self.last_error,
        }


# ── Event factory ──────────────────────────────────────────────────────────────

def _make_event(seq: int, event_type: str | None = None, extra_fields: dict | None = None) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    ev = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type or random.choice(EVENT_TYPES),
        "player_id": random.choice(PLAYERS),
        "match_id": random.choice(MATCHES),
        "card_name": random.choice(CARD_NAMES),
        "location": random.randint(1, 3),
        "snap_cubes": random.choice([1, 2, 4, 8]),
        "result": random.choice(["win", "loss", "retreat", "pending"]),
        "produced_at": now,
        "ingested_at": now,
        "host": _HOSTNAME,
        "sequence_num": seq,
    }
    if extra_fields:
        ev.update(extra_fields)
    return ev


# ── Producer Manager ───────────────────────────────────────────────────────────

class ProducerManager:
    def __init__(self) -> None:
        self.stats = ProducerStats()
        self._task: Optional[asyncio.Task] = None
        self._spike_task: Optional[asyncio.Task] = None
        self._unacked_events: List[dict] = []
        self._last_window_start: float = time.time()
        self._window_count: int = 0
        self._stream: Optional[Any] = None  # ZerobusStream when live

        config = get_zerobus_config()
        # We'll probe Zerobus on first start(); default to demo for now
        self.stats.demo_mode = not ZEROBUS_SDK_AVAILABLE

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self, rate: int = 5) -> None:
        if self.stats.state == ProducerState.RUNNING:
            return
        self.stats.state = ProducerState.RUNNING
        self.stats.rate = rate
        self._reset_rate_window()
        self._task = asyncio.create_task(self._produce_loop())

    async def stop(self) -> None:
        self.stats.state = ProducerState.STOPPED
        await self._cancel_task()
        await self._close_stream_gracefully()
        self._unacked_events.clear()

    async def kill(self) -> None:
        """Hard kill — no flush. Simulates process crash."""
        self.stats.acked_at_kill = self.stats.events_acked
        self.stats.unacked_at_kill = self.stats.events_in_flight
        self.stats.state = ProducerState.KILLED
        await self._cancel_task()
        # Deliberately do NOT close stream or flush — crash semantics.
        # WAL-committed events are safe; in-flight will replay on reconnect.

    async def resume(self) -> None:
        """Reconnect after crash, re-send unacked events (at-least-once)."""
        if self.stats.state not in (ProducerState.KILLED, ProducerState.STOPPED):
            return
        self.stats.state = ProducerState.RECONNECTING
        await asyncio.sleep(1.2)  # simulate TCP reconnect delay

        # At-least-once: replay unacked events first
        unacked_copy = list(self._unacked_events)
        for event in unacked_copy:
            self.stats.events_resent += 1
            self.stats.events_sent += 1
            self.stats.events_in_flight += 1
            self.stats.add_event_log(event, "resent")
            if self.stats.demo_mode:
                asyncio.create_task(self._simulate_ack(event, resend=True))
            elif self._stream:
                try:
                    self._stream.ingest_record_nowait(_make_proto_payload(event))
                except Exception:
                    pass

        self.stats.state = ProducerState.RUNNING
        self._reset_rate_window()
        self._task = asyncio.create_task(self._produce_loop())

    async def spike(self) -> None:
        """Crank to 500 events/sec for 5s then restore."""
        if self._spike_task and not self._spike_task.done():
            self._spike_task.cancel()
        self._spike_task = asyncio.create_task(self._do_spike())

    async def send_schema_violation(self) -> None:
        """Send event with unknown_field → triggers schema rejection in Zerobus."""
        self.stats.sequence_num += 1
        event = _make_event(
            self.stats.sequence_num,
            extra_fields={"unknown_field": "INVALID_SCHEMA", "extra_junk": 99999},
        )
        self.stats.events_sent += 1
        self.stats.events_in_flight += 1
        self.stats.add_event_log(event, "rejected")

        if not self.stats.demo_mode and self._stream:
            try:
                self._stream.ingest_record_nowait(_make_proto_payload(event))
            except Exception:
                pass

        asyncio.create_task(self._register_rejection())

    # ── Internals ──────────────────────────────────────────────────────────────

    def _reset_rate_window(self) -> None:
        self._last_window_start = time.time()
        self._window_count = 0

    async def _cancel_task(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _close_stream_gracefully(self) -> None:
        if self._stream:
            try:
                await asyncio.wait_for(self._stream.flush(), timeout=3.0)
                await asyncio.wait_for(self._stream.close(), timeout=3.0)
            except Exception:
                pass
            self._stream = None

    async def _do_spike(self) -> None:
        original = self.stats.rate
        self.stats.rate = 500
        try:
            await asyncio.sleep(5.0)
        finally:
            self.stats.rate = original

    async def _register_rejection(self) -> None:
        await asyncio.sleep(0.15)
        self.stats.events_in_flight = max(0, self.stats.events_in_flight - 1)
        self.stats.events_failed += 1
        self.stats.rejection_count += 1

    # ── Main produce loop ──────────────────────────────────────────────────────

    async def _produce_loop(self) -> None:
        try:
            if ZEROBUS_SDK_AVAILABLE:
                success = await self._produce_real()
                if not success:
                    # Zerobus unavailable / auth failed — fall back to in-memory demo
                    self.stats.demo_mode = True
                    await self._produce_demo()
            else:
                self.stats.demo_mode = True
                await self._produce_demo()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Producer loop error: {e}")
            self.stats.last_error = str(e)
            self.stats.state = ProducerState.STOPPED

    # ── Demo mode ──────────────────────────────────────────────────────────────

    async def _produce_demo(self) -> None:
        while True:
            rate = max(1, self.stats.rate)
            self.stats.sequence_num += 1
            event = _make_event(self.stats.sequence_num)
            self.stats.events_sent += 1
            self.stats.events_in_flight += 1
            self._unacked_events.append(event)
            self.stats.add_event_log(event, "sent")
            asyncio.create_task(self._simulate_ack(event))
            self._tick_rate()
            await asyncio.sleep(1.0 / rate)

    async def _simulate_ack(self, event: dict, resend: bool = False) -> None:
        """Simulate WAL ack with 50–150 ms latency."""
        await asyncio.sleep(random.uniform(0.05, 0.15))
        if self.stats.state in (
            ProducerState.RUNNING,
            ProducerState.RECONNECTING,
            ProducerState.STOPPED,
        ):
            self.stats.events_acked += 1
            self.stats.events_in_flight = max(0, self.stats.events_in_flight - 1)
            self.stats.delta_count += 1
            et = event.get("event_type", "unknown")
            self.stats.delta_by_type[et] = self.stats.delta_by_type.get(et, 0) + 1
            self._unacked_events = [
                e for e in self._unacked_events if e["event_id"] != event["event_id"]
            ]

    # ── Real Zerobus mode ──────────────────────────────────────────────────────

    async def _produce_real(self) -> bool:
        """
        Open a Zerobus stream and produce events into otto_demo.sd.zerobus_ingest.
        Returns False if connection fails (triggers demo mode fallback).

        Auth priority:
        1. DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET (injected by Databricks Apps)
           → proper OAuth2 M2M, gets correctly-scoped Zerobus token
        2. HeadersProvider with DATABRICKS_TOKEN (app SP runtime token, fallback)
        3. HeadersProvider with workspace client token (local dev, likely fails Zerobus auth)
        """
        config = get_zerobus_config()
        sdk = ZerobusSdk(  # type: ignore[name-defined]
            host=config["host"],
            unity_catalog_url=config["unity_catalog_url"],
        )
        props = TableProperties(config["table_name"], descriptor_proto=GAME_EVENT_DESCRIPTOR_BYTES)  # type: ignore[name-defined]
        ack_cb = _ZerobusAckCallback(self)
        opts = StreamConfigurationOptions(  # type: ignore[name-defined]
            record_type=RecordType.PROTO,  # type: ignore[name-defined]
            ack_callback=ack_cb,
            recovery=True,
            recovery_retries=5,
            max_inflight_records=10000,
        )

        zb_client_id = os.environ.get("ZEROBUS_CLIENT_ID", "")
        zb_client_secret = os.environ.get("ZEROBUS_CLIENT_SECRET", "")

        if zb_client_id and zb_client_secret:
            # Let the SDK handle Zerobus-specific OAuth internally
            print(f"Connecting to Zerobus at {config['host']} (SDK M2M: {zb_client_id[:8]}...)...")
            try:
                self._stream = await asyncio.wait_for(
                    sdk.create_stream(zb_client_id, zb_client_secret, props, opts),
                    timeout=15.0,
                )
            except Exception as e:
                err = str(e)
                print(f"Zerobus connect error (SDK M2M): {err}")
                self.stats.last_error = err[:500]
                return False
        else:
            # Fallback: HeadersProvider with DATABRICKS_TOKEN or workspace client token
            print(f"Connecting to Zerobus at {config['host']} (HeadersProvider fallback)...")
            try:
                hp = _DatabricksHeadersProvider()  # type: ignore[name-defined]
                self._stream = await asyncio.wait_for(
                    sdk.create_stream("", "", props, opts, headers_provider=hp),
                    timeout=15.0,
                )
            except Exception as e:
                err = str(e)
                print(f"Zerobus connect error (HeadersProvider): {err}")
                self.stats.last_error = err[:500]
                return False

        self.stats.demo_mode = False
        self.stats.last_error = ""
        print("Zerobus stream opened — producing live events")

        while True:
            rate = max(1, self.stats.rate)
            self.stats.sequence_num += 1
            event = _make_event(self.stats.sequence_num)
            payload = json.dumps(event).encode()

            try:
                self._stream.ingest_record_nowait(_make_proto_payload(event))
                self.stats.events_sent += 1
                self.stats.events_in_flight += 1
                self._unacked_events.append(event)
                self.stats.add_event_log(event, "sent")
            except Exception as e:
                print(f"ingest error: {e}")
                self.stats.events_failed += 1

            self._tick_rate()
            await asyncio.sleep(1.0 / rate)

    def _tick_rate(self) -> None:
        self._window_count += 1
        now = time.time()
        elapsed = now - self._last_window_start
        if elapsed >= 1.0:
            self.stats.events_per_sec = self._window_count / elapsed
            self._window_count = 0
            self._last_window_start = now


# ── WAL ack callback (real SDK) ────────────────────────────────────────────────

_AckCallbackBase = AckCallback if ZEROBUS_SDK_AVAILABLE else object  # type: ignore

class _ZerobusAckCallback(_AckCallbackBase):  # type: ignore
    """
    Called by the Rust WAL thread when each record offset is durably committed.
    on_ack(offset)  → safe to remove from in-flight buffer
    on_error(offset, msg) → record rejected (schema violation etc.)
    """

    def __init__(self, manager: ProducerManager) -> None:
        if ZEROBUS_SDK_AVAILABLE:
            super().__init__()
        self._mgr = manager

    def on_ack(self, offset: int) -> None:
        s = self._mgr.stats
        s.events_acked += 1
        s.events_in_flight = max(0, s.events_in_flight - 1)
        s.delta_count += 1
        # Pop oldest unacked event (FIFO)
        if self._mgr._unacked_events:
            ev = self._mgr._unacked_events.pop(0)
            et = ev.get("event_type", "unknown")
            s.delta_by_type[et] = s.delta_by_type.get(et, 0) + 1

    def on_error(self, offset: int, error_message: str) -> None:
        s = self._mgr.stats
        s.events_failed += 1
        s.events_in_flight = max(0, s.events_in_flight - 1)
        if "schema" in error_message.lower() or "rejected" in error_message.lower():
            s.rejection_count += 1
        print(f"Zerobus error at offset {offset}: {error_message}")


# Singleton
producer_manager = ProducerManager()
