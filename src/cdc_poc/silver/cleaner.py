"""Keep only row events: drop heartbeats, tombstones, truncates and messages."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from ..domain import HEARTBEAT_PREFIX, ROW_OPERATIONS


@dataclass(frozen=True, slots=True)
class CleanResult:
    rows: pl.DataFrame
    dropped: int


class RowEventFilter:
    def __init__(
        self, row_operations: frozenset[str] = ROW_OPERATIONS, heartbeat_prefix: str = HEARTBEAT_PREFIX
    ) -> None:
        self._ops = sorted(row_operations)
        self._heartbeat_prefix = heartbeat_prefix

    def apply(self, events: pl.DataFrame) -> CleanResult:
        rows = events.filter(
            pl.col("operation").is_in(self._ops) & ~pl.col("destination").str.starts_with(self._heartbeat_prefix)
        )
        return CleanResult(rows, events.height - rows.height)
