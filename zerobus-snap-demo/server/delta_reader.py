"""
Delta table reads via SQL Statement API (live) or in-memory simulation (demo mode).
Target table: configured via ZEROBUS_TABLE_NAME env var (default: otto_demo.sd.zerobus_ingest)
"""

import os
import aiohttp
from typing import List, Dict, Any

from .config import get_workspace_host, get_oauth_token, get_zerobus_config
from .producer import producer_manager


def _table() -> str:
    return os.environ.get("ZEROBUS_TABLE_NAME", "otto_demo.sd.zerobus_ingest")


class DeltaReader:
    def __init__(self) -> None:
        self._warehouse_id: str = ""

    def _demo_mode(self) -> bool:
        return producer_manager.stats.demo_mode

    # ── SQL helpers ────────────────────────────────────────────────────────────

    async def _execute_sql(self, sql: str) -> List[Dict[str, Any]]:
        try:
            config = get_zerobus_config()
            if not self._warehouse_id:
                self._warehouse_id = await self._get_warehouse_id()
                if not self._warehouse_id:
                    self._warehouse_id = config.get("warehouse_id", "")

            host = get_workspace_host()
            token = get_oauth_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            payload = {
                "statement": sql,
                "warehouse_id": self._warehouse_id,
                "format": "JSON_ARRAY",
                "wait_timeout": "30s",
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{host}/api/2.0/sql/statements/",
                    headers=headers,
                    json=payload,
                ) as resp:
                    result = await resp.json()
                    if resp.status != 200:
                        print(f"SQL error: {result}")
                        return []
                    if result.get("result") and result["result"].get("data_array"):
                        if result.get("manifest") and result["manifest"].get("schema"):
                            cols = [
                                c["name"] for c in result["manifest"]["schema"]["columns"]
                            ]
                            return [
                                dict(zip(cols, row))
                                for row in result["result"]["data_array"]
                            ]
                    return []
        except Exception as e:
            print(f"DeltaReader SQL error: {e}")
            return []

    async def _get_warehouse_id(self) -> str:
        try:
            host = get_workspace_host()
            token = get_oauth_token()
            headers = {"Authorization": f"Bearer {token}"}
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{host}/api/2.0/sql/warehouses", headers=headers
                ) as resp:
                    result = await resp.json()
                    warehouses = result.get("warehouses", [])
                    if warehouses:
                        return warehouses[0]["id"]
        except Exception as e:
            print(f"Warehouse lookup error: {e}")
        return ""

    # ── Public API ─────────────────────────────────────────────────────────────

    async def get_event_count(self) -> int:
        if self._demo_mode():
            return producer_manager.stats.delta_count
        rows = await self._execute_sql(
            f"SELECT COUNT(*) AS cnt FROM {_table()}"
        )
        return int(rows[0]["cnt"]) if rows else 0

    async def get_event_count_by_type(self) -> List[Dict[str, Any]]:
        if self._demo_mode():
            return [
                {"event_type": k, "count": v}
                for k, v in sorted(
                    producer_manager.stats.delta_by_type.items(),
                    key=lambda x: x[1],
                    reverse=True,
                )
            ]
        tbl = _table()
        rows = await self._execute_sql(
            f"SELECT event_type, COUNT(*) AS count "
            f"FROM {tbl} "
            f"GROUP BY event_type ORDER BY count DESC"
        )
        return rows

    async def get_recent_events(self, limit: int = 10) -> List[Dict[str, Any]]:
        if self._demo_mode():
            return list(reversed(producer_manager.stats.event_log[-limit:]))
        rows = await self._execute_sql(
            f"SELECT event_id, event_type, player_id, card_name, host, "
            f"produced_at, sequence_num "
            f"FROM {_table()} "
            f"ORDER BY produced_at DESC LIMIT {limit}"
        )
        return rows

    async def get_rejection_count(self) -> int:
        if self._demo_mode():
            return producer_manager.stats.rejection_count
        # Check _zerobus/table_rejected_parquets/ via DBFS API
        try:
            host = get_workspace_host()
            token = get_oauth_token()
            tbl = _table().replace(".", "/")
            path = f"/mnt/delta/{tbl}/_zerobus/table_rejected_parquets/"
            headers = {"Authorization": f"Bearer {token}"}
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{host}/api/2.0/dbfs/list",
                    headers=headers,
                    params={"path": path},
                ) as resp:
                    result = await resp.json()
                    files = result.get("files", [])
                    return len(
                        [f for f in files if str(f.get("path", "")).endswith(".parquet")]
                    )
        except Exception:
            return producer_manager.stats.rejection_count

    async def get_host_breakdown(self) -> List[Dict[str, Any]]:
        """Group events by producer host — shows multi-producer scenarios."""
        if self._demo_mode():
            from .producer import _HOSTNAME
            total = producer_manager.stats.delta_count
            return [{"host": _HOSTNAME, "count": total}] if total > 0 else []
        tbl = _table()
        rows = await self._execute_sql(
            f"SELECT host, COUNT(*) AS count FROM {tbl} "
            f"GROUP BY host ORDER BY count DESC LIMIT 10"
        )
        return rows


# Singleton
delta_reader = DeltaReader()
