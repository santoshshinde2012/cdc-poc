"""Composition root for the receiver: the only place that picks concrete classes."""

from __future__ import annotations

import logging
import os
from http.server import ThreadingHTTPServer

from ..clock import SystemClock
from ..config import ReceiverConfig
from ..logging_setup import configure_logging
from ..ports import BronzeSink, Clock
from ..storage.delta import DeltaBronzeWriter
from .envelope import EnvelopeMapper
from .faults import FaultInjector
from .http import make_handler, make_server
from .idempotency import Sha256IdempotencyKey
from .lifecycle import serve_until_signalled
from .service import IngestService

log = logging.getLogger("receiver")


def build_service(config: ReceiverConfig, sink: BronzeSink, clock: Clock) -> IngestService:
    mapper = EnvelopeMapper(config.table_keys, Sha256IdempotencyKey(), clock)
    return IngestService(mapper, sink, FaultInjector())


def build_server(
    config: ReceiverConfig, sink: BronzeSink | None = None, clock: Clock | None = None
) -> ThreadingHTTPServer:
    clock = clock or SystemClock()
    service = build_service(config, sink or DeltaBronzeWriter(config.bronze_path), clock)
    return make_server(config.host, config.port, make_handler(service, clock))


def main() -> None:
    configure_logging(os.environ)
    config = ReceiverConfig.from_env(os.environ)
    config.bronze_path.parent.mkdir(parents=True, exist_ok=True)
    log.info("receiver listening", extra={"host": config.host, "port": config.port, "bronze": str(config.bronze_path)})
    serve_until_signalled(build_server(config))


if __name__ == "__main__":
    main()
