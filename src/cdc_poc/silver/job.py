"""The Silver job: wires the steps together in the article's order. Holds no storage details."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import polars as pl

from ..ports import ChangeSource, Merger, SilverReader
from .cleaner import RowEventFilter
from .deduper import Deduper
from .ordering import LateEventFilter, LsnOrderer
from .projector import ImageProjector


@dataclass(slots=True)
class JobStats:
    bronze_rows_read: int = 0
    bronze_files_read: int = 0
    operations_read: dict[str, int] = field(default_factory=dict)
    dropped_non_row_events: int = 0
    dup_in_batch: int = 0
    dup_across_runs: int = 0
    superseded_in_batch: dict[str, int] = field(default_factory=dict)
    ignored_late: dict[str, int] = field(default_factory=dict)
    upserts: dict[str, int] = field(default_factory=dict)
    deletes: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"bronze_rows_read": self.bronze_rows_read, "bronze_files_read": self.bronze_files_read}
        if self.bronze_rows_read:
            out |= {
                "operations_read": self.operations_read,
                "dropped_non_row_events": self.dropped_non_row_events,
                "dup_in_batch": self.dup_in_batch,
                "dup_across_runs": self.dup_across_runs,
            }
        for name in ("superseded_in_batch", "ignored_late", "upserts", "deletes"):
            if value := getattr(self, name):
                out[name] = value
        return out


@dataclass(frozen=True, slots=True)
class SilverSteps:
    cleaner: RowEventFilter
    deduper: Deduper
    projector: ImageProjector
    orderer: LsnOrderer
    late_filter: LateEventFilter


class SilverJob:
    def __init__(
        self,
        source: ChangeSource,
        silver: SilverReader,
        merger: Merger,
        steps: SilverSteps,
        *,
        table_keys: Mapping[str, Sequence[str]],
        destinations: Mapping[str, str],
    ) -> None:
        self._source = source
        self._silver = silver
        self._merger = merger
        self._steps = steps
        self._table_keys = table_keys
        self._destinations = destinations

    def run(self) -> JobStats:
        stats = JobStats()
        batch = self._source.read_new()
        stats.bronze_rows_read, stats.bronze_files_read = batch.frame.height, batch.size
        if batch.frame.height:
            counts = batch.frame.group_by("operation").len().sort("operation")
            stats.operations_read = {str(op): int(n) for op, n in counts.iter_rows()}
            cleaned = self._steps.cleaner.apply(batch.frame)
            stats.dropped_non_row_events = cleaned.dropped
            deduped = self._steps.deduper.apply(cleaned.rows)
            stats.dup_in_batch, stats.dup_across_runs = deduped.in_batch, deduped.across_runs
            for table, keys in self._table_keys.items():
                self._apply_table(table, keys, deduped.rows, stats)
        self._source.commit(batch)
        return stats

    def _apply_table(self, table: str, keys: Sequence[str], rows: pl.DataFrame, stats: JobStats) -> None:
        events = rows.filter(pl.col("destination") == self._destinations[table])
        if events.height == 0:
            return
        images = self._steps.projector.project(events, self._silver.schema(table))
        latest = self._steps.orderer.latest_per_key(images, keys)
        stats.superseded_in_batch[table] = latest.dropped
        fresh = self._steps.late_filter.drop_late(table, latest.rows, keys)
        stats.ignored_late[table] = fresh.dropped
        if fresh.rows.height == 0:
            return
        stats.upserts[table] = fresh.rows.filter(pl.col("_op") != "delete").height
        stats.deletes[table] = fresh.rows.filter(pl.col("_op") == "delete").height
        self._merger.merge(table, fresh.rows, keys)
