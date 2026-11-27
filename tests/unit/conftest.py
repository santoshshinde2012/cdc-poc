from __future__ import annotations

from pathlib import Path

import pytest

from cdc_poc.clock import FakeClock
from cdc_poc.config import SilverConfig
from cdc_poc.ingest.envelope import EnvelopeMapper
from cdc_poc.ingest.idempotency import Sha256IdempotencyKey


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def mapper(clock: FakeClock) -> EnvelopeMapper:
    return EnvelopeMapper({"customers": ["id"], "orders": ["id"]}, Sha256IdempotencyKey(), clock)


@pytest.fixture
def silver_config(tmp_path: Path) -> SilverConfig:
    return SilverConfig(data_dir=tmp_path / "data")
