"""Domain vocabulary shared by ingest and Silver: operations and the Bronze envelope row."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any


class Operation(StrEnum):
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    TRUNCATE = "truncate"
    MESSAGE = "message"
    HEARTBEAT = "heartbeat"
    TOMBSTONE = "tombstone"


#: Debezium `op` codes to envelope operation names.
DEBEZIUM_OP_CODES: dict[str, Operation] = {
    "c": Operation.CREATE,
    "r": Operation.READ,
    "u": Operation.UPDATE,
    "d": Operation.DELETE,
    "t": Operation.TRUNCATE,
    "m": Operation.MESSAGE,
}

#: Operations that describe a row and therefore reach Silver.
ROW_OPERATIONS: frozenset[str] = frozenset({Operation.CREATE, Operation.READ, Operation.UPDATE, Operation.DELETE})

HEARTBEAT_DESTINATION = "__debezium-heartbeat.cdc"
HEARTBEAT_PREFIX = "__debezium-heartbeat"


@dataclass(frozen=True, slots=True)
class EnvelopeRow:
    """One Bronze row per change event. The column set never changes when the source does."""

    destination: str
    operation: str
    key: str | None
    before: str | None
    after: str | None
    value: str | None
    source_lsn: int | None
    source_sequence: str | None
    source_txid: int | None
    source_snapshot: str | None
    source_ts_ms: int | None
    event_ts_ms: int | None
    idempotency_key: str
    ingest_batch_id: str
    ingest_ts: datetime

    def as_record(self) -> dict[str, Any]:
        return asdict(self)
