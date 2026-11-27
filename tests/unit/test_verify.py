from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import polars as pl

from cdc_poc.storage.memory import InMemorySilverStore
from cdc_poc.verify.app import render
from cdc_poc.verify.differ import SilverVerifier
from cdc_poc.verify.normalize import norm


class FakeSource:
    def __init__(self, tables: dict[str, list[dict[str, Any]]]) -> None:
        self._tables = tables

    def rows(self, table: str) -> list[dict[str, Any]]:
        return self._tables[table]


def silver_with(**tables: pl.DataFrame) -> InMemorySilverStore:
    store = InMemorySilverStore()
    store.tables.update(tables)
    return store


SRC = [
    {"id": 1, "name": "a", "amount": Decimal("10.50"), "updated_at": "2026-01-01T10:00:00+00:00"},
    {"id": 2, "name": "b", "amount": Decimal("3.00"), "updated_at": "2026-01-02T10:00:00+00:00"},
]


def silver_frame(**override: Any) -> pl.DataFrame:
    rows = [
        {"id": 1, "name": "a", "amount": "10.50", "updated_at": datetime(2026, 1, 1, 10, tzinfo=UTC), "_lsn": 5},
        {"id": 2, "name": "b", "amount": "3.0", "updated_at": datetime(2026, 1, 2, 10, tzinfo=UTC), "_lsn": 6},
    ]
    if override:
        rows[1] = rows[1] | override
    return pl.DataFrame(rows)


def test_equal_when_values_match_after_normalizing_types() -> None:
    report = SilverVerifier(FakeSource({"t": SRC}), silver_with(t=silver_frame())).verify(["t"])
    assert report.ok
    assert report.tables[0].source_rows == report.tables[0].silver_rows == 2


def test_changed_value_is_reported_on_both_sides() -> None:
    report = SilverVerifier(FakeSource({"t": SRC}), silver_with(t=silver_frame(name="WRONG"))).verify(["t"])
    t = report.tables[0]
    assert not report.ok
    assert any("b" in r for r in t.only_in_source)
    assert any("WRONG" in r for r in t.only_in_silver)


def test_same_row_count_does_not_hide_a_difference() -> None:
    frame = silver_frame(amount="99.99")
    diff = SilverVerifier(FakeSource({"t": SRC}), silver_with(t=frame)).diff("t")
    assert diff.source_rows == diff.silver_rows
    assert not diff.equal


def test_missing_and_extra_rows() -> None:
    only_first = silver_frame().head(1)
    assert SilverVerifier(FakeSource({"t": SRC}), silver_with(t=only_first)).diff("t").only_in_source
    extra = pl.concat([silver_frame(), silver_frame().head(1).with_columns(id=pl.lit(9, dtype=pl.Int64))])
    assert SilverVerifier(FakeSource({"t": SRC}), silver_with(t=extra)).diff("t").only_in_silver


def test_empty_source_and_empty_silver_match_but_unbuilt_silver_does_not() -> None:
    assert SilverVerifier(FakeSource({"t": []}), silver_with()).diff("t").equal
    assert not SilverVerifier(FakeSource({"t": SRC}), silver_with()).diff("t").equal


def test_internal_columns_are_ignored() -> None:
    frame = silver_frame().with_columns(_lsn=pl.lit(1))
    assert SilverVerifier(FakeSource({"t": SRC}), silver_with(t=frame)).diff("t").equal


def test_norm_makes_postgres_and_silver_values_comparable() -> None:
    assert norm("10.50") == norm(Decimal("10.5")) == norm(10.5)
    assert norm("2026-01-01T12:00:00+02:00") == norm(datetime(2026, 1, 1, 10, tzinfo=UTC))
    assert norm(datetime(2026, 1, 1, 10)) == norm(datetime(2026, 1, 1, 10, tzinfo=UTC))
    assert norm(None) is None
    assert norm(True) is True
    assert norm("abc") == "abc"


def test_report_text_and_json() -> None:
    bad = SilverVerifier(FakeSource({"t": SRC}), silver_with(t=silver_frame(name="WRONG"))).verify(["t"])
    text = render(bad)
    assert "MISMATCH" in text
    assert "only in Postgres" in text
    assert "FAILED" in text
    assert bad.to_dict()["ok"] is False
    good = SilverVerifier(FakeSource({"t": SRC}), silver_with(t=silver_frame())).verify(["t"])
    assert "row by row" in render(good)
