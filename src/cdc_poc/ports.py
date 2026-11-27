"""Interfaces (ports). Services depend on these, never on Delta or HTTP directly.

Each protocol is small on purpose (interface segregation): a writer does not have to
know how to read, and a reader of Silver does not have to know how to merge.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

import polars as pl

from .domain import EnvelopeRow


class Clock(Protocol):
    def now(self) -> datetime: ...

    def sleep(self, seconds: float) -> None: ...


class IdempotencyKeyStrategy(Protocol):
    def key(self, destination: str, key: str | None, operation: str, position: str) -> str: ...


class BronzeSink(Protocol):
    """Appends envelope rows. Returning normally means the rows are durable (the ack point)."""

    def append(self, rows: Sequence[EnvelopeRow]) -> None: ...


@dataclass(frozen=True, slots=True)
class ChangeBatch:
    """New Bronze rows plus an opaque token that marks them processed once committed."""

    frame: pl.DataFrame
    token: frozenset[str] = field(default_factory=frozenset)

    @property
    def size(self) -> int:
        return len(self.token)


class ChangeSource(Protocol):
    """Incremental reader of the Bronze change feed."""

    def read_new(self) -> ChangeBatch: ...

    def commit(self, batch: ChangeBatch) -> None: ...


class CheckpointStore(Protocol):
    def load(self) -> frozenset[str]: ...

    def save(self, processed: frozenset[str]) -> None: ...


class SeenKeyStore(Protocol):
    """Idempotency keys already applied, so a replay in a later run is still removed."""

    def drop_seen(self, frame: pl.DataFrame) -> pl.DataFrame: ...

    def remember(self, keys: pl.Series) -> None: ...


class SilverReader(Protocol):
    """Read side of Silver: current rows, their schema, and delete markers (key, _lsn)."""

    def read(self, table: str) -> pl.DataFrame: ...

    def schema(self, table: str) -> dict[str, pl.DataType]: ...

    def delete_markers(self, table: str) -> pl.DataFrame: ...


class Merger(Protocol):
    """Write side of Silver: applies ordered changes (columns + `_op` + `_lsn`) as SCD Type 1."""

    def merge(self, table: str, changes: pl.DataFrame, keys: Sequence[str]) -> None: ...


class SourceReader(Protocol):
    """Reads the current rows of a source table (the system of record), used to verify Silver."""

    def rows(self, table: str) -> list[dict[str, Any]]: ...
