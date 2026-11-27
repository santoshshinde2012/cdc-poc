from __future__ import annotations

from cdc_poc.ingest.envelope import EnvelopeMapper
from cdc_poc.ingest.faults import FaultInjector
from cdc_poc.ingest.service import IngestService
from cdc_poc.storage.memory import InMemoryBronze

from .events import change, customer, heartbeat


def make(mapper: EnvelopeMapper) -> tuple[IngestService, InMemoryBronze]:
    bronze = InMemoryBronze()
    return IngestService(mapper, bronze, FaultInjector(), batch_ids=lambda: "batch"), bronze


def test_ack_only_after_append(mapper: EnvelopeMapper) -> None:
    service, bronze = make(mapper)
    out = service.ingest([change("c", "customers", 1, after=customer(1, "a")), heartbeat()])
    assert out.acked
    assert out.body == {"committed": 2}
    assert len(bronze.rows) == 2


def test_single_event_payload(mapper: EnvelopeMapper) -> None:
    service, bronze = make(mapper)
    assert service.ingest(change("c", "customers", 1, after=customer(1, "a"))).status == 200
    assert len(bronze.rows) == 1


def test_failed_write_is_not_acked(mapper: EnvelopeMapper) -> None:
    service, bronze = make(mapper)
    bronze.fail_next = OSError("disk full")
    out = service.ingest([change("c", "customers", 1, after=customer(1, "a"))])
    assert out.status == 500
    assert not out.acked
    assert bronze.rows == []
    assert service.stats()["failed_writes"] == 1


def test_reject_writes_nothing(mapper: EnvelopeMapper) -> None:
    service, bronze = make(mapper)
    service.configure_faults({"reject": True})
    assert service.ingest([heartbeat()]).status == 503
    assert bronze.rows == []
    assert service.stats()["rejected"] == 1


def test_lost_ack_after_commit_makes_a_duplicate_on_retry(mapper: EnvelopeMapper) -> None:
    service, bronze = make(mapper)
    service.configure_faults({"drop_acks": 1})
    batch = [change("u", "customers", 3, after=customer(1, "a"))]
    assert service.ingest(batch).status == 503  # written, ack lost
    assert service.ingest(batch).status == 200  # sender retries the same batch
    keys = [r.idempotency_key for r in bronze.rows]
    assert len(keys) == 2
    assert len(set(keys)) == 1


def test_heartbeat_only_batches_do_not_consume_ack_faults(mapper: EnvelopeMapper) -> None:
    service, bronze = make(mapper)
    service.configure_faults({"drop_acks": 1})
    assert service.ingest([heartbeat()]).status == 200  # not affected, fault still armed
    assert service.ingest([change("c", "customers", 1, after=customer(1, "a"))]).status == 503
    assert len(bronze.rows) == 2


def test_delayed_ack_is_reported_not_slept(mapper: EnvelopeMapper) -> None:
    service, bronze = make(mapper)
    service.configure_faults({"delay_acks": 1, "delay_s": 4})
    out = service.ingest([change("c", "customers", 1, after=customer(1, "a"))])
    assert out.status == 200
    assert out.delay_s == 4
    assert len(bronze.rows) == 1


def test_stats_keys(mapper: EnvelopeMapper) -> None:
    service, _ = make(mapper)
    service.ingest([heartbeat()])
    stats = service.stats()
    assert stats["batches"] == 1
    assert stats["events"] == 1
    assert {"dropped_acks", "delayed_acks", "rejected", "drop_acks", "reject"} <= stats.keys()
