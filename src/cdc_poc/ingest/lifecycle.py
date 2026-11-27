"""Graceful shutdown for the receiver: finish in-flight batches, then stop."""

from __future__ import annotations

import logging
import signal
import threading
from collections.abc import Callable
from http.server import ThreadingHTTPServer
from types import FrameType

log = logging.getLogger(__name__)


def make_stop_handler(server: ThreadingHTTPServer) -> Callable[[int, FrameType | None], None]:
    """A signal handler that stops serve_forever(). shutdown() blocks, so it runs on its own thread."""

    def handler(signum: int, _frame: FrameType | None) -> None:
        log.info("shutdown requested", extra={"signal": signal.Signals(signum).name})
        threading.Thread(target=server.shutdown, name="shutdown", daemon=True).start()

    return handler


def serve_until_signalled(server: ThreadingHTTPServer) -> None:
    """Serve until SIGTERM or SIGINT. Request threads are not daemons, so server_close() waits for
    a batch that is mid-commit: the sender then gets its ack (or a retry), never a half-written Bronze file."""
    server.daemon_threads = False
    handler = make_stop_handler(server)
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        log.info("receiver stopped")
