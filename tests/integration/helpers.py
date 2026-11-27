"""Helpers for the end-to-end tests: docker, psql, Bronze and Silver reads, comparison."""

from __future__ import annotations

import json
import re
import subprocess
import time
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib import request

import polars as pl
from deltalake import DeltaTable

from cdc_poc.clock import SystemClock
from cdc_poc.config import DEFAULT_TABLE_KEYS, SilverConfig
from cdc_poc.domain import EnvelopeRow
from cdc_poc.ingest.envelope import EnvelopeMapper
from cdc_poc.ingest.idempotency import Sha256IdempotencyKey
from cdc_poc.silver.app import build_delta
from cdc_poc.storage.delta import DeltaBronzeWriter

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
BRONZE = DATA / "bronze" / "events"
RESULTS = ROOT / "results"
RECEIVER = "http://localhost:18080"


def sh(*args: str, check: bool = True, timeout: int = 600) -> str:
    p = subprocess.run(list(args), cwd=ROOT, capture_output=True, text=True, timeout=timeout, check=False)
    if check and p.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed ({p.returncode}):\n{p.stdout}\n{p.stderr}")
    return p.stdout


def compose(*args: str, check: bool = True) -> str:
    return sh("docker", "compose", *args, check=check)


def psql(sql: str) -> str:
    return sh(
        "docker",
        "compose",
        "exec",
        "-T",
        "-e",
        "PGTZ=UTC",
        "postgres",
        "psql",
        "-U",
        "app",
        "-d",
        "shop",
        "-v",
        "ON_ERROR_STOP=1",
        "-At",
        "-c",
        sql,
    ).strip()


def psql_json(sql: str) -> Any:
    return json.loads(psql(sql) or "null")


def workload(n: int, seed: float) -> None:
    sh("./run.sh", "workload", str(n), str(seed))


def source_rows(table: str) -> list[dict[str, Any]]:
    return psql_json(f"SELECT coalesce(json_agg(t ORDER BY id), '[]') FROM public.{table} t") or []


def bronze() -> pl.DataFrame:
    if not (BRONZE / "_delta_log").exists():
        return pl.DataFrame()
    frame = pl.from_arrow(DeltaTable(str(BRONZE)).to_pyarrow_dataset().to_table())
    assert isinstance(frame, pl.DataFrame)
    return frame


def receiver(path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None if body is None else json.dumps(body).encode()
    req = request.Request(
        RECEIVER + path,
        data=data,
        method="POST" if body is not None else "GET",
        headers={"content-type": "application/json"},
    )
    with request.urlopen(req, timeout=10) as r:
        return dict(json.loads(r.read()))


_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}T")


def norm(v: Any) -> Any:  # noqa: PLR0911
    """Normalize values so Postgres JSON and Debezium JSON compare equal (numeric, timestamptz)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    if isinstance(v, str):
        if _ISO.match(v):
            try:
                return datetime.fromisoformat(v).astimezone(UTC)
            except ValueError:
                return v
        try:
            if re.fullmatch(r"-?\d+(\.\d+)?", v):
                return Decimal(v)
        except InvalidOperation:
            pass
    return v


def normalize(rows: list[dict[str, Any]], columns: list[str]) -> list[tuple[Any, ...]]:
    return sorted(tuple(norm(r.get(c)) for c in columns) for r in rows)


def diff_silver_vs_source(table: str, silver_df: pl.DataFrame) -> dict[str, Any]:
    src = source_rows(table)
    cols = sorted(src[0].keys()) if src else [c for c in silver_df.columns if not c.startswith("_")]
    sil = [dict(r.items()) for r in silver_df.to_dicts()] if silver_df.height else []
    a, b = normalize(src, cols), normalize(sil, cols)
    sa, sb = set(a), set(b)
    return {
        "table": table,
        "source_rows": len(a),
        "silver_rows": len(b),
        "columns": cols,
        "only_in_source": [list(map(str, x)) for x in sorted(sa - sb, key=str)[:5]],
        "only_in_silver": [list(map(str, x)) for x in sorted(sb - sa, key=str)[:5]],
        "equal": a == b,
    }


def wait_for[T](pred: Callable[[], T | None], timeout: float = 120, interval: float = 2, what: str = "condition") -> T:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = pred()
        if last:
            return last
        time.sleep(interval)
    raise TimeoutError(f"timed out after {timeout}s waiting for {what}; last={last!r}")


def lsn_max(series: pl.Series) -> int:
    value = series.max()
    return int(value) if isinstance(value, int) else 0


def lsn_min(series: pl.Series) -> int:
    value = series.min()
    return int(value) if isinstance(value, int) else 0


def current_wal_lsn() -> int:
    return int(psql("SELECT pg_current_wal_lsn() - '0/0'::pg_lsn"))


def bronze_caught_up(min_lsn: int) -> bool:
    """True once Bronze holds a row event at or beyond min_lsn (heartbeats excluded)."""
    df = bronze()
    if df.height == 0:
        return False
    rows = df.filter(pl.col("operation").is_in(["create", "update", "delete"]))
    return rows.height > 0 and lsn_max(rows["source_lsn"]) >= min_lsn


def write_result(name: str, obj: Any) -> None:
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / f"{name}.json").write_text(json.dumps(obj, indent=2, default=str))


# ---- Silver and Bronze through the package's own composition roots ----
SILVER_CONFIG = SilverConfig(data_dir=DATA)


def silver_run(full_refresh: bool = False) -> dict[str, Any]:
    return build_delta(SILVER_CONFIG, full_refresh=full_refresh).job.run().to_dict()


def read_silver(table: str) -> pl.DataFrame:
    return build_delta(SILVER_CONFIG).store.read(table)


def delete_markers(table: str) -> pl.DataFrame:
    return build_delta(SILVER_CONFIG).store.delete_markers(table)


def envelope_rows(events: list[dict[str, Any]], batch_id: str) -> list[EnvelopeRow]:
    mapper = EnvelopeMapper(DEFAULT_TABLE_KEYS, Sha256IdempotencyKey(), SystemClock())
    return mapper.map_batch(events, batch_id)


def append_bronze(rows: list[EnvelopeRow]) -> None:
    DeltaBronzeWriter(BRONZE).append(rows)
