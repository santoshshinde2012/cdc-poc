from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from typing import Any
from urllib import error, request

import pytest

from cdc_poc.clock import FakeClock
from cdc_poc.ingest.envelope import EnvelopeMapper
from cdc_poc.ingest.faults import FaultInjector
from cdc_poc.ingest.http import make_handler, make_server
from cdc_poc.ingest.service import IngestService
from cdc_poc.storage.memory import InMemoryBronze

from .events import change, customer


@pytest.fixture
def server(mapper: EnvelopeMapper, clock: FakeClock) -> Iterator[tuple[str, InMemoryBronze, FakeClock]]:
    bronze = InMemoryBronze()
    srv = make_server("127.0.0.1", 0, make_handler(IngestService(mapper, bronze, FaultInjector()), clock))
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{srv.server_address[1]}", bronze, clock
    srv.shutdown()
    srv.server_close()


def call(url: str, body: Any = None, raw: bytes | None = None) -> tuple[int, Any]:
    data = raw if raw is not None else (None if body is None else json.dumps(body).encode())
    req = request.Request(url, data=data, method="GET" if data is None else "POST")
    try:
        with request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read())
    except error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_health(server: tuple[str, InMemoryBronze, FakeClock]) -> None:
    assert call(server[0] + "/health") == (200, {"ok": True})


def test_ingest_batch(server: tuple[str, InMemoryBronze, FakeClock]) -> None:
    url, bronze, _ = server
    status, body = call(url + "/ingest", [change("c", "customers", 1, after=customer(1, "a"))])
    assert (status, body) == (200, {"committed": 1})
    assert len(bronze.rows) == 1


def test_bad_json_is_400(server: tuple[str, InMemoryBronze, FakeClock]) -> None:
    assert call(server[0] + "/ingest", raw=b"{nope")[0] == 400


def test_unknown_path_is_404(server: tuple[str, InMemoryBronze, FakeClock]) -> None:
    assert call(server[0] + "/nope")[0] == 404


def test_fault_endpoint_and_delay_uses_clock(server: tuple[str, InMemoryBronze, FakeClock]) -> None:
    url, bronze, clock = server
    status, stats = call(url + "/admin/fault", {"delay_acks": 1, "delay_s": 3})
    assert status == 200
    assert stats["delay_acks"] == 1
    assert call(url + "/ingest", [change("c", "customers", 1, after=customer(1, "a"))])[0] == 200
    assert clock.slept == [3.0]
    assert len(bronze.rows) == 1


def test_lost_ack_is_503_but_written(server: tuple[str, InMemoryBronze, FakeClock]) -> None:
    url, bronze, _ = server
    call(url + "/admin/fault", {"drop_acks": 1})
    assert call(url + "/ingest", [change("c", "customers", 1, after=customer(1, "a"))])[0] == 503
    assert len(bronze.rows) == 1
    assert call(url + "/stats")[1]["dropped_acks"] == 1
