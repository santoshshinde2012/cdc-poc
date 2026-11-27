"""Row-level comparison of the source tables against Silver."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from ..ports import SilverReader, SourceReader
from .normalize import normalize_rows

SAMPLE = 5


@dataclass(frozen=True, slots=True)
class TableDiff:
    table: str
    source_rows: int
    silver_rows: int
    columns: tuple[str, ...]
    only_in_source: tuple[tuple[str, ...], ...] = ()
    only_in_silver: tuple[tuple[str, ...], ...] = ()
    silver_built: bool = True

    @property
    def equal(self) -> bool:
        return self.silver_built and not self.only_in_source and not self.only_in_silver

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "source_rows": self.source_rows,
            "silver_rows": self.silver_rows,
            "columns": list(self.columns),
            "only_in_source": [list(r) for r in self.only_in_source],
            "only_in_silver": [list(r) for r in self.only_in_silver],
            "equal": self.equal,
        }


@dataclass(frozen=True, slots=True)
class VerifyReport:
    tables: tuple[TableDiff, ...] = field(default_factory=tuple)

    @property
    def ok(self) -> bool:
        return all(t.equal for t in self.tables)

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "tables": [t.to_dict() for t in self.tables]}


class SilverVerifier:
    """Compares every column of every row (not just counts). Columns come from the source rows."""

    def __init__(self, source: SourceReader, silver: SilverReader) -> None:
        self._source = source
        self._silver = silver

    def verify(self, tables: Sequence[str]) -> VerifyReport:
        return VerifyReport(tuple(self.diff(t) for t in tables))

    def diff(self, table: str) -> TableDiff:
        src = self._source.rows(table)
        frame = self._silver.read(table)
        columns = sorted(src[0]) if src else [c for c in frame.columns if not c.startswith("_")]
        silver = frame.to_dicts() if frame.height else []
        a, b = normalize_rows(src, columns), normalize_rows(silver, columns)
        sa, sb = set(a), set(b)
        return TableDiff(
            table=table,
            source_rows=len(a),
            silver_rows=len(b),
            columns=tuple(columns),
            only_in_source=_sample(sa - sb),
            only_in_silver=_sample(sb - sa),
            silver_built=frame.height > 0 or not src,
        )


def _sample(rows: set[tuple[Any, ...]]) -> tuple[tuple[str, ...], ...]:
    return tuple(tuple(map(str, r)) for r in sorted(rows, key=repr)[:SAMPLE])
