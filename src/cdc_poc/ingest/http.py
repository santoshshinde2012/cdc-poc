"""HTTP transport: maps requests to IngestService calls. No business rules here.

POST /ingest       one Debezium event or a JSON array of events (HTTP sink batch mode)
POST /admin/fault  fault settings, see faults.FaultSettings
GET  /health, GET /stats
"""

from __future__ import annotations

import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from ..ports import Clock
from .service import IngestService

log = logging.getLogger(__name__)


def make_handler(service: IngestService, clock: Clock) -> type[BaseHTTPRequestHandler]:
    """Build a handler class bound to its dependencies (no module-level state)."""

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            log.debug(format, *args)

        def _reply(self, code: int, body: Any) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self) -> bytes:
            length = int(self.headers.get("content-length") or 0)
            return self.rfile.read(length) if length else b""

        def do_GET(self) -> None:
            if self.path == "/health":
                self._reply(200, {"ok": True})
            elif self.path == "/stats":
                self._reply(200, service.stats())
            else:
                self._reply(404, {"error": "not found"})

        def do_POST(self) -> None:
            raw = self._body()
            if self.path == "/admin/fault":
                self._reply(200, service.configure_faults(json.loads(raw or b"{}")))
                return
            if self.path != "/ingest":
                self._reply(404, {"error": "not found"})
                return
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as exc:
                self._reply(400, {"error": f"bad json: {exc}"})
                return
            outcome = service.ingest(payload)
            if outcome.delay_s:
                clock.sleep(outcome.delay_s)  # outside the write lock
            self._reply(outcome.status, dict(outcome.body))

    return Handler


def make_server(host: str, port: int, handler: type[BaseHTTPRequestHandler]) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), handler)
