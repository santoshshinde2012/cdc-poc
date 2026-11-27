from __future__ import annotations

import polars as pl
import pytest

from cdc_poc.ingest.envelope import EnvelopeMapper
from cdc_poc.silver.cleaner import RowEventFilter
from cdc_poc.silver.deduper import Deduper
from cdc_poc.silver.ordering import LateEventFilter, LsnOrderer, NullSequenceError
from cdc_poc.silver.projector import ImageProjector
from cdc_poc.storage.bronze_schema import rows_to_frame
from cdc_poc.storage.memory import InMemorySeenKeyStore, InMemorySilverStore

from .events import change, customer, heartbeat


def bronze(mapper: EnvelopeMapper, *events: dict[str, object]) -> pl.DataFrame:
    return rows_to_frame(mapper.map_batch(list(events), "b"))


def test_cleaner_keeps_only_row_events(mapper: EnvelopeMapper) -> None:
    df = bronze(
        mapper,
        heartbeat(),
        change("c", "customers", 1, after=customer(1, "a")),
        change("t", "customers", 2),
        change("r", "orders", 3, after={"id": 1}),
    )
    out = RowEventFilter().apply(df)
    assert out.rows["operation"].to_list() == ["create", "read"]
    assert out.dropped == 2


def test_cleaner_drops_tombstone_rows(mapper: EnvelopeMapper) -> None:
    df = bronze(
        mapper,
        change("d", "customers", 4, before=customer(1, "a")),
        change("c", "customers", 5, after=customer(2, "b")),
    )
    # A tombstone row as the Zerobus sink would store it: same shape, operation "tombstone".
    df = df.with_columns(
        pl.when(pl.col("operation") == "delete")
        .then(pl.lit("tombstone"))
        .otherwise(pl.col("operation"))
        .alias("operation")
    )
    out = RowEventFilter().apply(df)
    assert out.rows["operation"].to_list() == ["create"]
    assert out.dropped == 1


def test_deduper_in_batch_and_across_runs(mapper: EnvelopeMapper) -> None:
    e1 = change("u", "customers", 5, after=customer(1, "a"))
    e2 = change("u", "customers", 6, after=customer(1, "b"))
    dedupe = Deduper(InMemorySeenKeyStore())
    first = dedupe.apply(bronze(mapper, e1, e1, e2))
    assert (first.rows.height, first.in_batch, first.across_runs) == (2, 1, 0)
    second = dedupe.apply(bronze(mapper, e2, change("u", "customers", 7, after=customer(1, "c"))))
    assert (second.rows.height, second.in_batch, second.across_runs) == (1, 0, 1)


def test_projector_types_and_delete_image(mapper: EnvelopeMapper) -> None:
    df = bronze(
        mapper,
        change("c", "orders", 1, after={"id": 1, "amount": "9.50", "meta": {"a": 1}}),
        change("d", "orders", 2, before={"id": 2, "amount": "1.00", "meta": None}),
    )
    out = ImageProjector().project(df, {})
    assert out.schema["id"] == pl.Int64
    assert out.schema["amount"] == pl.String
    assert out["meta"].to_list() == ['{"a": 1}', None]
    assert out.filter(pl.col("_op") == "delete")["id"].to_list() == [2]
    assert out["_lsn"].to_list() == [1, 2]


def test_projector_fills_columns_silver_already_has(mapper: EnvelopeMapper) -> None:
    df = bronze(mapper, change("u", "customers", 3, after=customer(1, "a")))
    out = ImageProjector().project(df, {"region": pl.String(), "id": pl.Int64()})
    assert out["region"].to_list() == [None]
    assert out.schema["region"] == pl.String


def images(*rows: tuple[int, int, str]) -> pl.DataFrame:
    return pl.DataFrame({"id": [r[0] for r in rows], "_lsn": [r[1] for r in rows], "_op": [r[2] for r in rows]})


def test_orderer_keeps_highest_lsn_per_key_regardless_of_arrival() -> None:
    out = LsnOrderer().latest_per_key(images((1, 30, "update"), (1, 20, "update"), (2, 5, "create")), ["id"])
    assert sorted(out.rows.select("id", "_lsn").rows()) == [(1, 30), (2, 5)]
    assert out.dropped == 1


def test_orderer_rejects_null_lsn() -> None:
    df = pl.DataFrame({"id": [1], "_lsn": [None], "_op": ["update"]}, schema_overrides={"_lsn": pl.Int64})
    with pytest.raises(NullSequenceError):
        LsnOrderer().latest_per_key(df, ["id"])


def test_late_filter_against_live_rows_and_delete_markers() -> None:
    store = InMemorySilverStore()
    store.tables["c"] = pl.DataFrame({"id": [1], "_lsn": [50], "name": ["x"]})
    store.markers["c"] = pl.DataFrame({"id": [2], "_lsn": [40]})
    out = LateEventFilter(store).drop_late(
        "c",
        images((1, 49, "update"), (2, 39, "create"), (3, 1, "create"), (1, 51, "update")).unique("id", keep="last"),
        ["id"],
    )
    assert sorted(out.rows["id"].to_list()) == [1, 3]
    assert out.dropped == 1


def test_late_filter_with_empty_silver_keeps_everything() -> None:
    out = LateEventFilter(InMemorySilverStore()).drop_late("c", images((1, 1, "create")), ["id"])
    assert (out.rows.height, out.dropped) == (1, 0)
