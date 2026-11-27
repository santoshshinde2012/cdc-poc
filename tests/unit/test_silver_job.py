"""The whole Silver job against both adapters. The same assertions must hold for Delta
and in-memory stores (Liskov: the job cannot tell them apart)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import polars as pl
import pytest

from cdc_poc.config import SilverConfig
from cdc_poc.ingest.envelope import EnvelopeMapper
from cdc_poc.ports import BronzeSink, SilverReader
from cdc_poc.silver.app import build_delta, build_in_memory
from cdc_poc.silver.job import SilverJob
from cdc_poc.storage.delta import DeltaBronzeWriter

from .events import change, customer, heartbeat


@dataclass
class Harness:
    mapper: EnvelopeMapper
    sink: BronzeSink
    job_factory: Callable[[bool], tuple[SilverJob, SilverReader]]

    def __post_init__(self) -> None:
        self.job, self.silver = self.job_factory(False)

    def write(self, *events: dict[str, Any]) -> None:
        self.sink.append(self.mapper.map_batch(list(events), "b"))

    def run(self) -> dict[str, Any]:
        return self.job.run().to_dict()

    def rebuild(self) -> dict[str, Any]:
        self.job, self.silver = self.job_factory(True)
        return self.run()

    def rows(self, table: str = "customers") -> list[tuple[Any, ...]]:
        df = self.silver.read(table)
        cols = sorted(c for c in df.columns if not c.startswith("_"))
        return sorted(df.select(cols).rows()) if df.height else []


@pytest.fixture(params=["memory", "delta"])
def h(request: pytest.FixtureRequest, mapper: EnvelopeMapper, silver_config: SilverConfig) -> Harness:
    if request.param == "memory":
        mem = build_in_memory(silver_config)

        def memory_factory(full_refresh: bool) -> tuple[SilverJob, SilverReader]:
            if full_refresh:
                rebuilt = build_in_memory(silver_config, mem.bronze)
                return rebuilt.job, rebuilt.store
            return mem.job, mem.store

        return Harness(mapper, mem.bronze, memory_factory)

    def delta_factory(full_refresh: bool) -> tuple[SilverJob, SilverReader]:
        built = build_delta(silver_config, full_refresh=full_refresh)
        return built.job, built.store

    return Harness(mapper, DeltaBronzeWriter(silver_config.bronze_path), delta_factory)


def snapshot(h: Harness) -> None:
    h.write(*(change("r", "customers", 10, after=customer(i, f"c{i}"), snapshot="true") for i in (1, 2, 3)))


def test_empty_bronze(h: Harness) -> None:
    assert h.run() == {"bronze_rows_read": 0, "bronze_files_read": 0}


def test_snapshot_then_changes(h: Harness) -> None:
    snapshot(h)
    h.write(
        change("u", "customers", 20, after=customer(1, "c1", tier="gold")),
        change("d", "customers", 21, before=customer(2, "c2")),
        change("c", "customers", 22, after=customer(4, "c4")),
        heartbeat(),
    )
    stats = h.run()
    assert stats["operations_read"] == {"create": 1, "delete": 1, "heartbeat": 1, "read": 3, "update": 1}
    assert stats["dropped_non_row_events"] == 1
    assert [r[1] for r in h.rows()] == [1, 3, 4]  # id column (email, id, name, tier)
    assert ("c1@example.com", 1, "c1", "gold") in h.rows()


def test_incremental_runs_read_only_new_files(h: Harness) -> None:
    snapshot(h)
    assert h.run()["bronze_files_read"] == 1
    assert h.run()["bronze_rows_read"] == 0
    h.write(change("u", "customers", 20, after=customer(1, "new")))
    stats = h.run()
    assert (stats["bronze_rows_read"], stats["upserts"]) == (1, {"customers": 1})


def test_replay_duplicates_do_not_change_silver(h: Harness) -> None:
    snapshot(h)
    batch = [
        change("u", "customers", 20, after=customer(1, "x")),
        change("d", "customers", 21, before=customer(3, "c3")),
    ]
    h.write(*batch)
    h.write(*batch)  # lost ack: same batch again in the same run
    first = h.run()
    h.write(*batch)  # crash replay: same batch again in a later run
    second = h.run()
    assert first["dup_in_batch"] == 2
    assert second["dup_across_runs"] == 2
    assert [r[2] for r in h.rows()] == ["x", "c2"]


def test_late_lower_lsn_events_are_ignored(h: Harness) -> None:
    snapshot(h)
    h.write(
        change("u", "customers", 30, after=customer(1, "new")), change("d", "customers", 31, before=customer(2, "c2"))
    )
    h.run()
    h.write(
        change("u", "customers", 29, after=customer(1, "STALE")),
        change("c", "customers", 25, after=customer(2, "RESURRECTED")),
    )
    stats = h.run()
    assert stats["ignored_late"] == {"customers": 2}
    assert [r[2] for r in h.rows()] == ["new", "c3"]


def test_later_lsn_in_same_batch_wins_even_if_it_arrived_first(h: Harness) -> None:
    snapshot(h)
    h.write(change("u", "customers", 40, after=customer(1, "newer")))
    h.write(change("u", "customers", 35, after=customer(1, "older")))
    stats = h.run()
    assert stats["superseded_in_batch"] == {"customers": 2}  # snapshot row and the older update
    assert ("c1@example.com", 1, "newer", "basic") in h.rows()


def test_delete_then_reinsert(h: Harness) -> None:
    snapshot(h)
    h.write(change("d", "customers", 20, before=customer(1, "c1")))
    h.run()
    h.write(change("c", "customers", 21, after=customer(1, "back")))
    h.run()
    assert ("c1@example.com", 1, "back", "basic") in h.rows()


def test_schema_evolution_adds_column(h: Harness) -> None:
    snapshot(h)
    h.run()
    h.write(change("u", "customers", 50, after=customer(1, "c1", region="EU")))
    h.run()
    df = h.silver.read("customers")
    assert "region" in df.columns
    assert dict(zip(df["id"].to_list(), df["region"].to_list(), strict=True)) == {1: "EU", 2: None, 3: None}


def test_full_refresh_rebuilds_same_state(h: Harness) -> None:
    snapshot(h)
    h.write(
        change("u", "customers", 20, after=customer(1, "x")), change("d", "customers", 21, before=customer(2, "c2"))
    )
    h.run()
    h.write(change("u", "customers", 19, after=customer(1, "STALE")))
    h.run()
    before = h.rows()
    h.rebuild()
    assert h.rows() == before


def test_tables_are_independent(h: Harness) -> None:
    h.write(
        change("c", "orders", 5, after={"id": 1, "amount": "2.00"}), change("c", "customers", 6, after=customer(1, "a"))
    )
    h.run()
    assert h.silver.read("orders").height == 1
    assert h.silver.read("customers").height == 1
    assert isinstance(h.silver.read("orders"), pl.DataFrame)
