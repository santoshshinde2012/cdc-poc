"""Delta Lake adapters (delta-rs). Every class implements one port from cdc_poc.ports."""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake

from ..domain import EnvelopeRow
from ..ports import ChangeBatch, CheckpointStore
from .bronze_schema import rows_to_arrow


def is_delta_table(path: Path) -> bool:
    return (path / "_delta_log").exists()


def read_delta(path: Path, columns: Sequence[str] | None = None) -> pl.DataFrame:
    table = DeltaTable(str(path)).to_pyarrow_dataset().to_table(columns=list(columns) if columns else None)
    frame = pl.from_arrow(table)
    assert isinstance(frame, pl.DataFrame)
    return frame


class DeltaBronzeWriter:
    """BronzeSink: one Delta commit per batch. `append` returns after the commit (durable ack point)."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def append(self, rows: Sequence[EnvelopeRow]) -> None:
        if rows:
            write_deltalake(str(self._path), rows_to_arrow(rows), mode="append")


class FileCheckpointStore:
    """CheckpointStore: the set of Bronze files already processed, written atomically."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self) -> frozenset[str]:
        if not self._path.exists():
            return frozenset()
        return frozenset(json.loads(self._path.read_text()).get("files", []))

    def save(self, processed: frozenset[str]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"files": sorted(processed)}))
        tmp.replace(self._path)


class DeltaBronzeReader:
    """ChangeSource: reads only Bronze data files not processed yet (Bronze is append-only)."""

    def __init__(self, path: Path, checkpoints: CheckpointStore) -> None:
        self._path = path
        self._checkpoints = checkpoints

    def read_new(self) -> ChangeBatch:
        if not is_delta_table(self._path):
            return ChangeBatch(pl.DataFrame())
        done = self._checkpoints.load()
        new = [f for f in DeltaTable(str(self._path)).file_uris() if os.path.basename(f) not in done]
        if not new:
            return ChangeBatch(pl.DataFrame())
        tables = [pq.read_table(f.removeprefix("file://")) for f in new]
        frame = pl.from_arrow(pa.concat_tables(tables, promote_options="default"))
        assert isinstance(frame, pl.DataFrame)
        return ChangeBatch(frame, frozenset(os.path.basename(f) for f in new))

    def commit(self, batch: ChangeBatch) -> None:
        self._checkpoints.save(self._checkpoints.load() | batch.token)


class DeltaSeenKeyStore:
    """SeenKeyStore backed by a one-column Delta table of idempotency keys."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def drop_seen(self, frame: pl.DataFrame) -> pl.DataFrame:
        if not is_delta_table(self._path) or frame.height == 0:
            return frame
        seen = read_delta(self._path, ["idempotency_key"])
        return frame.join(seen, on="idempotency_key", how="anti")

    def remember(self, keys: pl.Series) -> None:
        if keys.len():
            write_deltalake(str(self._path), pl.DataFrame({"idempotency_key": keys}).to_arrow(), mode="append")


class DeltaSilverStore:
    """SilverReader over Delta tables: <root>/<table> and <root>/_deleted_<table>."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def table_path(self, table: str) -> Path:
        return self.root / table

    def markers_path(self, table: str) -> Path:
        return self.root / f"_deleted_{table}"

    def read(self, table: str) -> pl.DataFrame:
        path = self.table_path(table)
        return read_delta(path) if is_delta_table(path) else pl.DataFrame()

    def schema(self, table: str) -> dict[str, pl.DataType]:
        return dict(self.read(table).schema)

    def delete_markers(self, table: str) -> pl.DataFrame:
        path = self.markers_path(table)
        return read_delta(path) if is_delta_table(path) else pl.DataFrame()


class DeltaScd1Merger:
    """Merger: Delta MERGE with SCD Type 1 rules and a new-column-friendly schema merge.

    - delete removes the row, other operations upsert, only when the change's LSN is higher;
    - delete markers remember the LSN of each delete and are dropped when the key returns.
    """

    def __init__(self, store: DeltaSilverStore) -> None:
        self._store = store

    def merge(self, table: str, changes: pl.DataFrame, keys: Sequence[str]) -> None:
        target = self._store.table_path(table)
        pred = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        upserts = changes.filter(pl.col("_op") != "delete")
        deletes = changes.filter(pl.col("_op") == "delete")
        if not is_delta_table(target):
            write_deltalake(str(target), upserts.drop("_op").to_arrow(), mode="append")
        else:
            (
                DeltaTable(str(target))
                .merge(changes.to_arrow(), predicate=pred, source_alias="s", target_alias="t", merge_schema=True)
                .when_matched_delete(predicate="s._op = 'delete' AND s._lsn > t._lsn")
                .when_matched_update_all(predicate="s._op <> 'delete' AND s._lsn > t._lsn", except_cols=["_op"])
                .when_not_matched_insert_all(predicate="s._op <> 'delete'", except_cols=["_op"])
                .execute()
            )
        self._merge_markers(table, changes, deletes, keys, pred)

    def _merge_markers(
        self, table: str, changes: pl.DataFrame, deletes: pl.DataFrame, keys: Sequence[str], pred: str
    ) -> None:
        markers = self._store.markers_path(table)
        cols = [*keys, "_lsn"]
        if not is_delta_table(markers):
            if deletes.height:
                write_deltalake(str(markers), deletes.select(cols).to_arrow(), mode="append")
            return
        (
            DeltaTable(str(markers))
            .merge(changes.select([*cols, "_op"]).to_arrow(), predicate=pred, source_alias="s", target_alias="t")
            .when_matched_update(predicate="s._op = 'delete'", updates={"_lsn": "s._lsn"})
            .when_matched_delete(predicate="s._op <> 'delete'")
            .when_not_matched_insert(
                predicate="s._op = 'delete'", updates={**{k: f"s.{k}" for k in keys}, "_lsn": "s._lsn"}
            )
            .execute()
        )
