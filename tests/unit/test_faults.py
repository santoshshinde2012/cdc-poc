from __future__ import annotations

from cdc_poc.ingest.faults import AfterCommit, FaultInjector


def test_default_injects_nothing() -> None:
    f = FaultInjector()
    assert not f.should_reject()
    assert f.after_commit() == AfterCommit()


def test_drop_acks_counts_down() -> None:
    f = FaultInjector()
    f.configure({"drop_acks": 2})
    assert [f.after_commit().drop for _ in range(3)] == [True, True, False]
    assert f.snapshot()["dropped_acks"] == 2


def test_delay_acks() -> None:
    f = FaultInjector()
    f.configure({"delay_acks": 1, "delay_s": 2.5})
    assert f.after_commit() == AfterCommit(delay_s=2.5)
    assert f.after_commit() == AfterCommit()
    assert f.snapshot()["delayed_acks"] == 1


def test_reject_until_cleared() -> None:
    f = FaultInjector()
    f.configure({"reject": True})
    assert f.should_reject()
    assert f.should_reject()
    f.configure({"reject": False})
    assert not f.should_reject()
    assert f.snapshot()["rejected"] == 2


def test_drop_wins_over_delay() -> None:
    f = FaultInjector()
    f.configure({"drop_acks": 1, "delay_acks": 1, "delay_s": 1})
    assert f.after_commit().drop
    assert f.after_commit().delay_s == 1
