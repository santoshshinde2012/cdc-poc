from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any

import pytest

from .helpers import RESULTS, ROOT, bronze, compose, sh, wait_for

SUMMARY: dict[str, Any] = {}


@pytest.fixture(scope="session")
def summary() -> dict[str, Any]:
    return SUMMARY


@pytest.fixture(scope="session", autouse=True)
def stack() -> Iterator[None]:
    """Fresh stack for the whole run: reset, up, wait for the initial snapshot."""
    RESULTS.mkdir(exist_ok=True)
    sh("./run.sh", "reset")
    sh("./run.sh", "up", timeout=900)
    sh("./run.sh", "seed")
    t0 = time.time()
    wait_for(lambda: bronze().height >= 150, timeout=240, what="initial snapshot in Bronze")
    SUMMARY["snapshot_seconds_after_up"] = round(time.time() - t0, 1)
    yield
    # keep evidence, then stop containers (named volume and ./data are kept)
    (RESULTS / "debezium.log").write_text(compose("logs", "--no-color", "debezium", check=False))
    rlog = ROOT / "data" / "receiver.log"
    if rlog.exists():
        (RESULTS / "receiver.log").write_text(rlog.read_text())
    (RESULTS / "summary.json").write_text(json.dumps(SUMMARY, indent=2, default=str))
    sh("./run.sh", "down", check=False)
