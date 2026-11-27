"""Remove duplicates on idempotency_key, inside a batch and against earlier runs."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from ..ports import SeenKeyStore


@dataclass(frozen=True, slots=True)
class DedupeResult:
    rows: pl.DataFrame
    in_batch: int
    across_runs: int


class Deduper:
    def __init__(self, seen: SeenKeyStore) -> None:
        self._seen = seen

    def apply(self, rows: pl.DataFrame) -> DedupeResult:
        unique = rows.unique(subset=["idempotency_key"], keep="first", maintain_order=True)
        fresh = self._seen.drop_seen(unique)
        if fresh.height:
            self._seen.remember(fresh["idempotency_key"])
        return DedupeResult(fresh, rows.height - unique.height, unique.height - fresh.height)
