"""Sequence by the source LSN: latest event per key wins, late events are ignored."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import polars as pl

from ..ports import SilverReader


class NullSequenceError(ValueError):
    """An event without a source LSN cannot be sequenced (AUTO CDC rejects NULL sequences too)."""


@dataclass(frozen=True, slots=True)
class OrderResult:
    rows: pl.DataFrame
    dropped: int


class LsnOrderer:
    """SEQUENCE BY _lsn: keep the highest-LSN event per key within a batch."""

    def latest_per_key(self, rows: pl.DataFrame, keys: Sequence[str]) -> OrderResult:
        nulls = rows["_lsn"].null_count()
        if nulls:
            raise NullSequenceError(f"{nulls} events have NULL source_lsn, cannot sequence")
        latest = rows.sort("_lsn", maintain_order=True).group_by(list(keys), maintain_order=True).last()
        return OrderResult(latest, rows.height - latest.height)


class LateEventFilter:
    """Drop events whose LSN is not above what Silver already applied for the key (live or deleted)."""

    def __init__(self, silver: SilverReader) -> None:
        self._silver = silver

    def drop_late(self, table: str, latest: pl.DataFrame, keys: Sequence[str]) -> OrderResult:
        keys = list(keys)
        applied = [
            f.select([*keys, "_lsn"])
            for f in (self._silver.read(table), self._silver.delete_markers(table))
            if f.height or f.width
        ]
        if not applied:
            return OrderResult(latest, 0)
        current = pl.concat(applied, how="vertical_relaxed").group_by(keys).agg(pl.col("_lsn").max().alias("_cur_lsn"))
        joined = latest.join(current, on=keys, how="left")
        is_late = pl.col("_cur_lsn").is_not_null() & (pl.col("_lsn") <= pl.col("_cur_lsn"))
        kept = joined.filter(~is_late).drop("_cur_lsn")
        return OrderResult(kept, latest.height - kept.height)
