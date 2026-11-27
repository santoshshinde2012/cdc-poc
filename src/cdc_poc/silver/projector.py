"""Turn envelope rows into typed row images: after (or before, for deletes) plus _op and _lsn."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import polars as pl


class ImageProjector:
    def project(self, events: pl.DataFrame, existing_schema: Mapping[str, pl.DataType]) -> pl.DataFrame:
        records = [
            self._record(op, after, before, lsn)
            for op, after, before, lsn in events.select("operation", "after", "before", "source_lsn").iter_rows()
        ]
        # Columns Silver already has but this batch's images lack (events from before an ADD COLUMN).
        columns = sorted({c for r in records for c in r} | set(existing_schema))
        return pl.DataFrame([self._series(c, [r.get(c) for r in records], existing_schema) for c in columns])

    @staticmethod
    def _record(op: str, after: str | None, before: str | None, lsn: int | None) -> dict[str, Any]:
        if op == "delete":
            image: dict[str, Any] = dict(json.loads(before) or {}) if before else {}  # key comes from before
        else:
            image = json.loads(after) if after is not None else {}
        image["_op"] = op
        image["_lsn"] = lsn
        return image

    @staticmethod
    def _series(name: str, values: list[Any], existing: Mapping[str, pl.DataType]) -> pl.Series:
        if name in existing:
            return pl.Series(name, values, dtype=existing[name])
        ints = all(v is None or (isinstance(v, int) and not isinstance(v, bool)) for v in values)
        if ints and any(v is not None for v in values):
            return pl.Series(name, values, dtype=pl.Int64)
        text = [None if v is None else (v if isinstance(v, str) else json.dumps(v)) for v in values]
        return pl.Series(name, text, dtype=pl.String)
