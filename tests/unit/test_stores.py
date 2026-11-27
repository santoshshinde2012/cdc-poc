"""Adapter contract tests: each store behaves the same for the operations its port defines."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from cdc_poc.config import ReceiverConfig, SilverConfig
from cdc_poc.ingest.envelope import EnvelopeMapper
from cdc_poc.ports import ChangeSource, CheckpointStore, SeenKeyStore
from cdc_poc.storage.bronze_schema import BRONZE_SCHEMA
from cdc_poc.storage.delta import (
    DeltaBronzeReader,
    DeltaBronzeWriter,
    DeltaScd1Merger,
    DeltaSeenKeyStore,
    DeltaSilverStore,
    FileCheckpointStore,
)
from cdc_poc.storage.memory import (
    InMemoryBronze,
    InMemoryBronzeReader,
    InMemoryCheckpointStore,
    InMemoryScd1Merger,
    InMemorySeenKeyStore,
    InMemorySilverStore,
)

from .events import change, customer


@pytest.fixture(params=["memory", "file"])
def checkpoints(request: pytest.FixtureRequest, tmp_path: Path) -> CheckpointStore:
    return InMemoryCheckpointStore() if request.param == "memory" else FileCheckpointStore(tmp_path / "cp.json")


def test_checkpoint_round_trip(checkpoints: CheckpointStore) -> None:
    assert checkpoints.load() == frozenset()
    checkpoints.save(frozenset({"a", "b"}))
    assert checkpoints.load() == {"a", "b"}


@pytest.fixture(params=["memory", "delta"])
def seen(request: pytest.FixtureRequest, tmp_path: Path) -> SeenKeyStore:
    return InMemorySeenKeyStore() if request.param == "memory" else DeltaSeenKeyStore(tmp_path / "seen")


def test_seen_keys(seen: SeenKeyStore) -> None:
    frame = pl.DataFrame({"idempotency_key": ["a", "b", "c"]})
    assert seen.drop_seen(frame).height == 3
    seen.remember(pl.Series(["a", "c"]))
    assert seen.drop_seen(frame)["idempotency_key"].to_list() == ["b"]


def test_bronze_read_new_and_commit(mapper: EnvelopeMapper, tmp_path: Path) -> None:
    rows = mapper.map_batch([change("c", "customers", 1, after=customer(1, "a"))], "b")
    writer = DeltaBronzeWriter(tmp_path / "bronze")
    memory = InMemoryBronze()
    sources: list[ChangeSource] = [
        DeltaBronzeReader(tmp_path / "bronze", InMemoryCheckpointStore()),
        InMemoryBronzeReader(memory, InMemoryCheckpointStore()),
    ]
    for sink in (writer, memory):
        sink.append(rows)
        sink.append([])  # an empty batch writes nothing
    for source in sources:
        batch = source.read_new()
        assert batch.frame.height == 1
        assert batch.size == 1
        assert batch.frame.columns == BRONZE_SCHEMA.names
        source.commit(batch)
        assert source.read_new().frame.height == 0


def test_delta_reader_on_missing_table(tmp_path: Path) -> None:
    assert DeltaBronzeReader(tmp_path / "none", InMemoryCheckpointStore()).read_new().frame.height == 0


def changes(*rows: tuple[int, int, str, str | None]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": [r[0] for r in rows],
            "_lsn": [r[1] for r in rows],
            "_op": [r[2] for r in rows],
            "name": [r[3] for r in rows],
        },
        schema_overrides={"name": pl.String},
    )


@pytest.mark.parametrize("kind", ["memory", "delta"])
def test_merger_contract(kind: str, tmp_path: Path) -> None:
    store: InMemorySilverStore | DeltaSilverStore
    if kind == "memory":
        store = InMemorySilverStore()
        merger: InMemoryScd1Merger | DeltaScd1Merger = InMemoryScd1Merger(store)
    else:
        store = DeltaSilverStore(tmp_path / "silver")
        merger = DeltaScd1Merger(store)
    merger.merge("t", changes((1, 10, "create", "a"), (2, 10, "create", "b"), (9, 10, "delete", None)), ["id"])
    merger.merge("t", changes((1, 20, "update", "a2"), (2, 5, "update", "OLD"), (3, 20, "create", "c")), ["id"])
    merger.merge("t", changes((3, 30, "delete", None)), ["id"])
    rows = sorted(store.read("t").select("id", "name", "_lsn").rows())
    assert rows == [(1, "a2", 20), (2, "b", 10)]
    assert sorted(store.delete_markers("t").select("id", "_lsn").rows()) == [(3, 30), (9, 10)]
    merger.merge("t", changes((3, 40, "create", "back")), ["id"])
    assert sorted(store.delete_markers("t").select("id", "_lsn").rows()) == [(9, 10)]
    assert (3, "back", 40) in store.read("t").select("id", "name", "_lsn").rows()


def test_config_from_env(tmp_path: Path) -> None:
    rc = ReceiverConfig.from_env({"BRONZE_PATH": "/x/b", "PORT": "1234"})
    assert (str(rc.bronze_path), rc.port, rc.host) == ("/x/b", 1234, "0.0.0.0")
    sc = SilverConfig.from_env({}, tmp_path)
    assert sc.bronze_path == tmp_path / "data" / "bronze" / "events"
    assert sc.destination("orders") == "cdc.public.orders"
    assert sorted(sc.table_keys) == ["customers", "orders"]
