"""Fault injection for tests: lose an ack, delay an ack, or refuse writes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(slots=True)
class FaultSettings:
    drop_acks: int = 0  # commit, then answer 503 (ack lost after a durable write)
    delay_acks: int = 0  # commit, then wait `delay_s` before the 2xx
    delay_s: float = 0.0
    reject: bool = False  # answer 503 without writing (sink unavailable)


@dataclass(slots=True)
class FaultCounters:
    dropped_acks: int = 0
    delayed_acks: int = 0
    rejected: int = 0


@dataclass(frozen=True, slots=True)
class AfterCommit:
    """What to do with the ack of a batch that is already committed."""

    drop: bool = False
    delay_s: float = 0.0


class FaultInjector:
    """Holds fault settings and decides per batch. Not thread safe: the caller serializes."""

    def __init__(self, settings: FaultSettings | None = None) -> None:
        self.settings = settings or FaultSettings()
        self.counters = FaultCounters()

    def configure(self, body: Mapping[str, Any]) -> None:
        s = self.settings
        if "drop_acks" in body:
            s.drop_acks = int(body["drop_acks"])
        if "delay_acks" in body:
            s.delay_acks = int(body["delay_acks"])
        if "delay_s" in body:
            s.delay_s = float(body["delay_s"])
        if "reject" in body:
            s.reject = bool(body["reject"])

    def should_reject(self) -> bool:
        if self.settings.reject:
            self.counters.rejected += 1
            return True
        return False

    def after_commit(self) -> AfterCommit:
        s = self.settings
        if s.drop_acks > 0:
            s.drop_acks -= 1
            self.counters.dropped_acks += 1
            return AfterCommit(drop=True)
        if s.delay_acks > 0:
            s.delay_acks -= 1
            self.counters.delayed_acks += 1
            return AfterCommit(delay_s=s.delay_s)
        return AfterCommit()

    def snapshot(self) -> dict[str, Any]:
        return {**asdict(self.settings), **asdict(self.counters)}
