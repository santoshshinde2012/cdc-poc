from __future__ import annotations

import json

from cdc_poc.clock import FakeClock
from cdc_poc.domain import HEARTBEAT_DESTINATION
from cdc_poc.ingest.envelope import EnvelopeMapper, canonical_json

from .events import change, customer, heartbeat


def test_create_maps_to_envelope(mapper: EnvelopeMapper, clock: FakeClock) -> None:
    row = mapper.map(change("c", "customers", 100, after=customer(1, "a")), "b1")
    assert row.destination == "cdc.public.customers"
    assert row.operation == "create"
    assert row.key == '{"id":1}'
    assert row.source_lsn == 100
    assert row.source_txid == 1
    assert row.source_snapshot == "false"
    assert row.before is None
    assert json.loads(row.after or "")["name"] == "a"
    assert row.ingest_batch_id == "b1"
    assert row.ingest_ts == clock.now()


def test_all_op_codes(mapper: EnvelopeMapper) -> None:
    ops = {code: mapper.map(change(code, "customers", 1, after=customer(1, "a")), "b").operation for code in "cruetm"}
    assert ops == {"c": "create", "r": "read", "u": "update", "e": "e", "t": "truncate", "m": "message"}


def test_delete_takes_key_from_before(mapper: EnvelopeMapper) -> None:
    row = mapper.map(change("d", "customers", 5, before=customer(9, "gone")), "b")
    assert row.operation == "delete"
    assert row.key == '{"id":9}'
    assert row.after is None


def test_heartbeat(mapper: EnvelopeMapper) -> None:
    row = mapper.map(heartbeat(42), "b")
    assert row.destination == HEARTBEAT_DESTINATION
    assert row.operation == "heartbeat"
    assert row.event_ts_ms == 42
    assert row.source_lsn is None


def test_unknown_table_has_no_key(mapper: EnvelopeMapper) -> None:
    assert mapper.map(change("c", "other", 1, after={"x": 1}), "b").key is None


def test_redelivery_gets_same_idempotency_key(mapper: EnvelopeMapper) -> None:
    event = change("u", "customers", 7, after=customer(1, "a"))
    assert mapper.map(event, "b1").idempotency_key == mapper.map(event, "b2").idempotency_key


def test_map_batch_skips_null_values(mapper: EnvelopeMapper) -> None:
    rows = mapper.map_batch([change("c", "customers", 1, after=customer(1, "a")), None, heartbeat()], "b")
    assert [r.operation for r in rows] == ["create", "heartbeat"]


def test_canonical_json_is_stable() -> None:
    assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'
    assert canonical_json(None) is None
