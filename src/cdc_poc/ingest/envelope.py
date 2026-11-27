"""Event parsing: one Debezium JSON change event -> one Bronze envelope row."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from ..domain import DEBEZIUM_OP_CODES, HEARTBEAT_DESTINATION, EnvelopeRow, Operation
from ..ports import Clock, IdempotencyKeyStrategy


def canonical_json(obj: Any) -> str | None:
    """Stable JSON (sorted keys, no spaces) so equal values give equal strings and keys."""
    if obj is None:
        return None
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


class EnvelopeMapper:
    def __init__(
        self,
        table_keys: Mapping[str, Sequence[str]],
        key_strategy: IdempotencyKeyStrategy,
        clock: Clock,
    ) -> None:
        self._table_keys = table_keys
        self._keys = key_strategy
        self._clock = clock

    def map_batch(self, events: Iterable[Mapping[str, Any] | None], batch_id: str) -> list[EnvelopeRow]:
        return [self.map(e, batch_id) for e in events if e is not None]

    def map(self, event: Mapping[str, Any], batch_id: str) -> EnvelopeRow:
        if "op" not in event:
            return self._heartbeat(event, batch_id)
        return self._change(event, batch_id)

    def _heartbeat(self, event: Mapping[str, Any], batch_id: str) -> EnvelopeRow:
        # Heartbeat events carry only {"ts_ms": ...} and no destination over HTTP.
        ts = event.get("ts_ms")
        return EnvelopeRow(
            destination=HEARTBEAT_DESTINATION,
            operation=Operation.HEARTBEAT.value,
            key=None,
            before=None,
            after=None,
            value=canonical_json(event),
            source_lsn=None,
            source_sequence=None,
            source_txid=None,
            source_snapshot=None,
            source_ts_ms=None,
            event_ts_ms=ts,
            idempotency_key=self._keys.key(HEARTBEAT_DESTINATION, None, Operation.HEARTBEAT.value, str(ts)),
            ingest_batch_id=batch_id,
            ingest_ts=self._clock.now(),
        )

    def _change(self, event: Mapping[str, Any], batch_id: str) -> EnvelopeRow:
        source: Mapping[str, Any] = event.get("source") or {}
        table = str(source.get("table", "unknown"))
        destination = f"cdc.{source.get('schema', 'public')}.{table}"
        op_code = event.get("op")
        operation = DEBEZIUM_OP_CODES[op_code].value if op_code in DEBEZIUM_OP_CODES else str(op_code)
        before, after = event.get("before"), event.get("after")
        lsn = source.get("lsn")
        position = canonical_json({"lsn": lsn, "sequence": source.get("sequence"), "txId": source.get("txId")})
        key = self._row_key(table, after if after is not None else before)
        snapshot = source.get("snapshot")
        return EnvelopeRow(
            destination=destination,
            operation=operation,
            key=key,
            before=canonical_json(before),
            after=canonical_json(after),
            value=canonical_json(event),
            source_lsn=lsn,
            source_sequence=source.get("sequence"),
            source_txid=source.get("txId"),
            source_snapshot=None if snapshot is None else str(snapshot),
            source_ts_ms=source.get("ts_ms"),
            event_ts_ms=event.get("ts_ms"),
            idempotency_key=self._keys.key(destination, key, operation, position or ""),
            ingest_batch_id=batch_id,
            ingest_ts=self._clock.now(),
        )

    def _row_key(self, table: str, image: Mapping[str, Any] | None) -> str | None:
        columns = self._table_keys.get(table)
        if image is None or columns is None:
            return None
        return canonical_json({c: image.get(c) for c in columns})
