"""In-memory adapters for the same ports. Used by unit tests; behave like the Delta ones."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

from ..domain import EnvelopeRow
from ..ports import ChangeBatch, CheckpointStore
from .bronze_schema import rows_to_frame


class InMemoryBronze:
    """BronzeSink over a list of committed batches (one named 'file' per append)."""

    def __init__(self) -> None:
        self._batches: list[tuple[str, list[EnvelopeRow]]] = []
        self.fail_next: Exception | None = None  # lets a test make one append fail before commit

    def append(self, rows: Sequence[EnvelopeRow]) -> None:
        if self.fail_next is not None:
            exc, self.fail_next = self.fail_next, None
            raise exc
        if rows:
            self._batches.append((f"batch-{len(self._batches):06d}", list(rows)))

    @property
    def batches(self) -> list[tuple[str, list[EnvelopeRow]]]:
        return list(self._batches)

    @property
    def rows(self) -> list[EnvelopeRow]:
        return [r for _, batch in self._batches for r in batch]


class InMemoryBronzeReader:
    """ChangeSource over InMemoryBronze; progress lives in a CheckpointStore, like DeltaBronzeReader."""

    def __init__(self, bronze: InMemoryBronze, checkpoints: CheckpointStore) -> None:
        self._bronze = bronze
        self._checkpoints = checkpoints

    def read_new(self) -> ChangeBatch:
        done = self._checkpoints.load()
        new = [(name, rows) for name, rows in self._bronze.batches if name not in done]
        if not new:
            return ChangeBatch(pl.DataFrame())
        return ChangeBatch(rows_to_frame([r for _, rows in new for r in rows]), frozenset(n for n, _ in new))

    def commit(self, batch: ChangeBatch) -> None:
        self._checkpoints.save(self._checkpoints.load() | batch.token)


class InMemoryCheckpointStore:
    def __init__(self) -> None:
        self._processed: frozenset[str] = frozenset()

    def load(self) -> frozenset[str]:
        return self._processed

    def save(self, processed: frozenset[str]) -> None:
        self._processed = processed


class InMemorySeenKeyStore:
    def __init__(self) -> None:
        self._keys: set[str] = set()

    def drop_seen(self, frame: pl.DataFrame) -> pl.DataFrame:
        if not self._keys or frame.height == 0:
            return frame
        return frame.filter(~pl.col("idempotency_key").is_in(sorted(self._keys)))

    def remember(self, keys: pl.Series) -> None:
        self._keys.update(keys.to_list())


class InMemorySilverStore:
    """SilverReader over dicts of DataFrames."""

    def __init__(self) -> None:
        self.tables: dict[str, pl.DataFrame] = {}
        self.markers: dict[str, pl.DataFrame] = {}

    def read(self, table: str) -> pl.DataFrame:
        return self.tables.get(table, pl.DataFrame())

    def schema(self, table: str) -> dict[str, pl.DataType]:
        return dict(self.read(table).schema)

    def delete_markers(self, table: str) -> pl.DataFrame:
        return self.markers.get(table, pl.DataFrame())


class InMemoryScd1Merger:
    """Merger with the same SCD Type 1 rules as DeltaScd1Merger, done with polars joins."""

    def __init__(self, store: InMemorySilverStore) -> None:
        self._store = store

    def merge(self, table: str, changes: pl.DataFrame, keys: Sequence[str]) -> None:
        keys = list(keys)
        target = self._store.tables.get(table)
        if target is None:
            self._store.tables[table] = changes.filter(pl.col("_op") != "delete").drop("_op")
        else:
            current = target.select([*keys, pl.col("_lsn").alias("_t_lsn")])
            joined = changes.join(current, on=keys, how="left")
            effective = joined.filter(pl.col("_t_lsn").is_null() | (pl.col("_lsn") > pl.col("_t_lsn")))
            # Rows replaced or removed by an effective change leave the target first.
            changed_keys = effective.filter(pl.col("_t_lsn").is_not_null()).select(keys)
            kept = target.join(changed_keys, on=keys, how="anti")
            upserts = effective.filter(pl.col("_op") != "delete").drop("_op", "_t_lsn")
            self._store.tables[table] = pl.concat([kept, upserts], how="diagonal_relaxed")
        self._merge_markers(table, changes, keys)

    def _merge_markers(self, table: str, changes: pl.DataFrame, keys: list[str]) -> None:
        deletes = changes.filter(pl.col("_op") == "delete").select([*keys, "_lsn"])
        markers = self._store.markers.get(table)
        if markers is None:
            if deletes.height:
                self._store.markers[table] = deletes
            return
        kept = markers.join(changes.select(keys), on=keys, how="anti")
        self._store.markers[table] = pl.concat([kept, deletes], how="vertical_relaxed")
