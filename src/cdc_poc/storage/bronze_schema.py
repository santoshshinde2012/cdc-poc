"""The Bronze envelope as an Arrow schema, shared by every Bronze store so reads look the same."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl
import pyarrow as pa

from ..domain import EnvelopeRow

BRONZE_SCHEMA = pa.schema(
    [
        pa.field("destination", pa.string(), nullable=False),
        pa.field("operation", pa.string(), nullable=False),
        pa.field("key", pa.string()),
        pa.field("before", pa.string()),
        pa.field("after", pa.string()),
        pa.field("value", pa.string()),
        pa.field("source_lsn", pa.int64()),
        pa.field("source_sequence", pa.string()),
        pa.field("source_txid", pa.int64()),
        pa.field("source_snapshot", pa.string()),
        pa.field("source_ts_ms", pa.int64()),
        pa.field("event_ts_ms", pa.int64()),
        pa.field("idempotency_key", pa.string(), nullable=False),
        pa.field("ingest_batch_id", pa.string(), nullable=False),
        pa.field("ingest_ts", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


def rows_to_arrow(rows: Sequence[EnvelopeRow]) -> pa.Table:
    return pa.Table.from_pylist([r.as_record() for r in rows], schema=BRONZE_SCHEMA)


def rows_to_frame(rows: Sequence[EnvelopeRow]) -> pl.DataFrame:
    frame = pl.from_arrow(rows_to_arrow(rows))
    assert isinstance(frame, pl.DataFrame)
    return frame
