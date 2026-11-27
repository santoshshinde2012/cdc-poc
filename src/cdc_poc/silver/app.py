"""Composition root for the Silver job: Delta or in-memory adapters, chosen here only."""

from __future__ import annotations

import argparse
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from ..config import SilverConfig
from ..ports import ChangeSource, Merger, SeenKeyStore, SilverReader
from ..storage.delta import (
    DeltaBronzeReader,
    DeltaScd1Merger,
    DeltaSeenKeyStore,
    DeltaSilverStore,
    FileCheckpointStore,
)
from ..storage.memory import (
    InMemoryBronze,
    InMemoryBronzeReader,
    InMemoryCheckpointStore,
    InMemoryScd1Merger,
    InMemorySeenKeyStore,
    InMemorySilverStore,
)
from .cleaner import RowEventFilter
from .deduper import Deduper
from .job import SilverJob, SilverSteps
from .ordering import LateEventFilter, LsnOrderer
from .projector import ImageProjector

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def build_job(
    config: SilverConfig, source: ChangeSource, silver: SilverReader, merger: Merger, seen: SeenKeyStore
) -> SilverJob:
    steps = SilverSteps(RowEventFilter(), Deduper(seen), ImageProjector(), LsnOrderer(), LateEventFilter(silver))
    destinations = {t: config.destination(t) for t in config.table_keys}
    return SilverJob(source, silver, merger, steps, table_keys=config.table_keys, destinations=destinations)


@dataclass(frozen=True, slots=True)
class DeltaSilver:
    job: SilverJob
    store: DeltaSilverStore


def build_delta(config: SilverConfig, full_refresh: bool = False) -> DeltaSilver:
    if full_refresh and config.silver_root.exists():
        shutil.rmtree(config.silver_root)  # Silver, its checkpoint and seen keys are all rebuildable
    store = DeltaSilverStore(config.silver_root)
    source = DeltaBronzeReader(config.bronze_path, FileCheckpointStore(config.checkpoint_path))
    job = build_job(config, source, store, DeltaScd1Merger(store), DeltaSeenKeyStore(config.seen_keys_path))
    return DeltaSilver(job, store)


@dataclass(frozen=True, slots=True)
class MemorySilver:
    job: SilverJob
    bronze: InMemoryBronze
    store: InMemorySilverStore


def build_in_memory(config: SilverConfig, bronze: InMemoryBronze | None = None) -> MemorySilver:
    bronze = bronze or InMemoryBronze()
    store = InMemorySilverStore()
    source = InMemoryBronzeReader(bronze, InMemoryCheckpointStore())
    job = build_job(config, source, store, InMemoryScd1Merger(store), InMemorySeenKeyStore())
    return MemorySilver(job, bronze, store)


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply new Bronze events to Silver.")
    parser.add_argument("--full-refresh", action="store_true", help="drop Silver and rebuild it from all of Bronze")
    args = parser.parse_args()
    config = SilverConfig.from_env(os.environ, PROJECT_ROOT)
    silver = build_delta(config, full_refresh=args.full_refresh)
    print(json.dumps(silver.job.run().to_dict(), indent=2, default=str))
    for table in config.table_keys:
        print(f"silver.{table}: {silver.store.read(table).height} rows")


if __name__ == "__main__":
    main()
