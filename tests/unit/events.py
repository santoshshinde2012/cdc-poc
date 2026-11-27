"""Builders for Debezium-shaped events used across unit tests."""

from __future__ import annotations

from typing import Any


def change(
    op: str,
    table: str,
    lsn: int,
    *,
    after: dict[str, Any] | None = None,
    before: dict[str, Any] | None = None,
    snapshot: str = "false",
    tx: int = 1,
) -> dict[str, Any]:
    return {
        "op": op,
        "before": before,
        "after": after,
        "ts_ms": 1_700_000_000_000 + lsn,
        "source": {
            "schema": "public",
            "table": table,
            "lsn": lsn,
            "sequence": f'[null,"{lsn}"]',
            "txId": tx,
            "snapshot": snapshot,
            "ts_ms": 1_700_000_000_000,
        },
    }


def customer(cid: int, name: str, tier: str = "basic", **extra: Any) -> dict[str, Any]:
    return {"id": cid, "name": name, "email": f"c{cid}@example.com", "tier": tier, **extra}


def heartbeat(ts: int = 1) -> dict[str, Any]:
    return {"ts_ms": ts}
