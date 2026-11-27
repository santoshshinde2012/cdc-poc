"""Ack handling: write the batch, and acknowledge only after the write is durable."""

from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from ..domain import Operation
from ..ports import BronzeSink
from .envelope import EnvelopeMapper
from .faults import AfterCommit, FaultInjector

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IngestOutcome:
    """Transport-neutral result: an HTTP layer turns this into a status and a body."""

    status: int
    body: Mapping[str, Any] = field(default_factory=dict)
    delay_s: float = 0.0

    @property
    def acked(self) -> bool:
        return 200 <= self.status < 300


@dataclass(slots=True)
class IngestCounters:
    batches: int = 0
    events: int = 0
    failed_writes: int = 0


class IngestService:
    def __init__(
        self,
        mapper: EnvelopeMapper,
        sink: BronzeSink,
        faults: FaultInjector,
        batch_ids: Callable[[], str] = lambda: str(uuid.uuid4()),
    ) -> None:
        self._mapper = mapper
        self._sink = sink
        self._faults = faults
        self._batch_ids = batch_ids
        self._lock = threading.Lock()  # one writer, one commit per batch
        self.counters = IngestCounters()

    def ingest(self, payload: Any) -> IngestOutcome:
        events = payload if isinstance(payload, list) else [payload]
        with self._lock:
            if self._faults.should_reject():
                return IngestOutcome(503, {"error": "injected: sink unavailable, nothing written"})
            try:
                rows = self._mapper.map_batch(events, self._batch_ids())
                if rows:
                    self._sink.append(rows)  # returns only after the commit
            except Exception as exc:  # no commit, no ack: the sender retries
                self.counters.failed_writes += 1
                log.exception("bronze write failed")
                return IngestOutcome(500, {"error": str(exc)})
            self.counters.batches += 1
            self.counters.events += len(rows)
            # Ack faults model a lost ack for data. Heartbeat-only batches (every few seconds)
            # must not use them up, or a test cannot tell which batch was affected.
            has_changes = any(r.operation != Operation.HEARTBEAT.value for r in rows)
            decision = self._faults.after_commit() if has_changes else AfterCommit()
        if decision.drop:
            log.warning("fault injection: ack dropped after commit", extra={"events": len(rows), "status": 503})
            return IngestOutcome(503, {"error": "injected: ack lost after durable write"})
        if decision.delay_s:
            log.warning("fault injection: ack delayed", extra={"events": len(rows), "delay_s": decision.delay_s})
        else:
            log.info("committed to bronze", extra={"events": len(rows)})
        return IngestOutcome(200, {"committed": len(rows)}, delay_s=decision.delay_s)

    def configure_faults(self, body: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._faults.configure(body)
            return self.stats_unlocked()

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return self.stats_unlocked()

    def stats_unlocked(self) -> dict[str, Any]:
        c = self.counters
        return {"batches": c.batches, "events": c.events, "failed_writes": c.failed_writes, **self._faults.snapshot()}
