"""Make Postgres values and Silver values comparable (numeric strings, timestamptz, UTC)."""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}T")
_NUMBER = re.compile(r"-?\d+(\.\d+)?")


def norm(value: Any) -> Any:  # noqa: PLR0911
    """Return a canonical form: numbers as Decimal, timestamps as aware UTC datetimes."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, str):
        if _ISO.match(value):
            try:
                return datetime.fromisoformat(value).astimezone(UTC)
            except ValueError:
                return value
        if _NUMBER.fullmatch(value):
            return Decimal(value)
    return value


def normalize_rows(rows: Sequence[dict[str, Any]], columns: Sequence[str]) -> list[tuple[Any, ...]]:
    """Rows as sorted tuples over `columns`, so two row sets compare with ==."""
    return sorted((tuple(norm(r.get(c)) for c in columns) for r in rows), key=repr)
