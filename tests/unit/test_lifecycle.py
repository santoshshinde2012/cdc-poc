from __future__ import annotations

import signal
import threading
import time

from cdc_poc.clock import FakeClock
from cdc_poc.ingest.envelope import EnvelopeMapper
from cdc_poc.ingest.faults import FaultInjector
from cdc_poc.ingest.http import make_handler, make_server
from cdc_poc.ingest.lifecycle import make_stop_handler
from cdc_poc.ingest.service import IngestService
from cdc_poc.storage.memory import InMemoryBronze


def test_signal_handler_stops_serve_forever(mapper: EnvelopeMapper, clock: FakeClock) -> None:
    service = IngestService(mapper, InMemoryBronze(), FaultInjector())
    server = make_server("127.0.0.1", 0, make_handler(service, clock))
    stopped = threading.Event()

    def serve() -> None:
        server.serve_forever()
        stopped.set()

    thread = threading.Thread(target=serve)
    thread.start()
    time.sleep(0.2)
    make_stop_handler(server)(signal.SIGTERM, None)
    assert stopped.wait(timeout=5), "serve_forever did not return after the stop handler ran"
    thread.join(timeout=5)
    server.server_close()
