"""End-to-end tests for the local push-based CDC proof of concept.

They run in file order against one stack and share state on purpose:
snapshot -> changes -> replays -> late event -> schema change -> slot retention.
"""

from __future__ import annotations

import json
import re
import subprocess
import time
from typing import Any

import polars as pl
import pytest

from .helpers import (
    ROOT,
    append_bronze,
    bronze,
    bronze_caught_up,
    compose,
    current_wal_lsn,
    delete_markers,
    diff_silver_vs_source,
    envelope_rows,
    lsn_max,
    lsn_min,
    psql,
    read_silver,
    receiver,
    run_cli,
    sh,
    silver_run,
    source_rows,
    wait_for,
    workload,
    write_result,
)

pytestmark = pytest.mark.integration

TABLES = ["customers", "orders"]


def converge(timeout: float = 180) -> dict[str, Any]:
    """Run the Silver job until Silver equals the source for every table."""
    last = {}

    def check() -> dict[str, Any] | None:
        nonlocal last
        stats = silver_run()
        diffs = {t: diff_silver_vs_source(t, read_silver(t)) for t in TABLES}
        last = {"stats": stats, "diffs": diffs}
        return last if all(d["equal"] for d in diffs.values()) else None

    try:
        return wait_for(check, timeout=timeout, interval=3, what="Silver == source")
    except TimeoutError as exc:
        raise AssertionError(f"Silver did not converge: {json.dumps(last, default=str)[:3000]}") from exc


def dup_count() -> int:
    df = bronze().filter(pl.col("operation") != "heartbeat")
    return df.height - df["idempotency_key"].n_unique()


def bronze_counts() -> dict[str, int]:
    df = bronze()
    return {
        f"{d}/{o}": n
        for d, o, n in df.group_by("destination", "operation").len().sort("destination", "operation").iter_rows()
    }


# (0) the article's open question: do snapshot rows carry a non-null source.lsn?
def test_01_snapshot_rows_carry_lsn(summary: dict[str, Any]) -> None:
    df = bronze()
    reads = df.filter(pl.col("operation") == "read")
    assert reads.height == 150, "50 customers + 100 orders expected as snapshot reads"
    first = json.loads(reads["value"][0])["source"]
    result: dict[str, Any] = {
        "snapshot_read_rows": reads.height,
        "null_source_lsn": reads["source_lsn"].null_count(),
        "distinct_snapshot_lsns": reads["source_lsn"].unique().to_list(),
        "snapshot_lsn_pg_format": psql(f"SELECT '0/0'::pg_lsn + {reads['source_lsn'][0]}"),
        "source_sequence_example": first.get("sequence"),
        "source_snapshot_values": sorted(reads["source_snapshot"].unique().to_list()),
        "source_example": first,
    }
    summary["01_snapshot_lsn"] = result
    write_result("01_snapshot_lsn", result)
    assert result["null_source_lsn"] == 0


# (1) snapshot + inserts/updates/deletes land, Silver equals the source
def test_02_changes_converge(summary: dict[str, Any]) -> None:
    workload(300, 0.11)
    res = converge()
    df = bronze()
    reads_lsn = lsn_max(df.filter(pl.col("operation") == "read")["source_lsn"])
    stream = df.filter(pl.col("operation").is_in(["create", "update", "delete"]))
    result: dict[str, Any] = {
        "bronze_counts": bronze_counts(),
        "silver_rows": {t: res["diffs"][t]["silver_rows"] for t in TABLES},
        "source_rows": {t: res["diffs"][t]["source_rows"] for t in TABLES},
        "min_streaming_lsn": lsn_min(stream["source_lsn"]),
        "snapshot_lsn": reads_lsn,
        "streaming_lsn_above_snapshot_lsn": lsn_min(stream["source_lsn"]) > reads_lsn,
        "tombstone_rows_in_bronze": df.filter(pl.col("operation") == "tombstone").height,
        "heartbeat_rows_in_bronze": df.filter(pl.col("operation") == "heartbeat").height,
        "delete_rows_with_null_after": df.filter((pl.col("operation") == "delete") & pl.col("after").is_null()).height,
        "delete_rows": df.filter(pl.col("operation") == "delete").height,
        "duplicates_in_bronze": dup_count(),
    }
    summary["02_changes"] = result
    write_result("02_changes", result)
    assert result["streaming_lsn_above_snapshot_lsn"]
    assert result["delete_rows"] > 0
    assert result["delete_rows"] == result["delete_rows_with_null_after"]


# (2a) replay: the receiver commits, then the ack is lost (503). Debezium resends the same batch.
def test_03_replay_after_lost_ack(summary: dict[str, Any]) -> None:
    before = dup_count()
    receiver("/admin/fault", {"drop_acks": 3})
    workload(80, 0.22)
    res = converge()
    stats = receiver("/stats")
    after = dup_count()
    result: dict[str, Any] = {
        "receiver_dropped_acks": stats["dropped_acks"],
        "bronze_duplicates_before": before,
        "bronze_duplicates_after": after,
        "new_duplicate_rows": after - before,
        "silver_equals_source": all(d["equal"] for d in res["diffs"].values()),
        "silver_rows": {t: res["diffs"][t]["silver_rows"] for t in TABLES},
    }
    summary["03_lost_ack"] = result
    write_result("03_lost_ack", result)
    assert stats["dropped_acks"] == 3
    assert after > before, "a lost ack must produce duplicate rows in Bronze"


# (2b) replay: receiver restart mid-stream; Debezium crash after the write but before the ack;
#      Debezium crash while the sink refuses writes (offsets and slot must not move past unacked events)
def offsets_file_lsn() -> int | None:
    raw = (ROOT / "data" / "debezium" / "offsets.dat").read_bytes().decode("latin-1")
    m = re.findall(r'"lsn":(\d+)', raw)
    return int(m[-1]) if m else None


def slot_confirmed_lsn() -> int:
    return int(psql("SELECT confirmed_flush_lsn - '0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name='dbz_slot'"))


def row_max_lsn() -> int:
    return lsn_max(bronze().filter(pl.col("operation").is_in(["create", "update", "delete"]))["source_lsn"])


def settle(min_wait: float = 25, timeout: float = 150) -> dict[str, Any]:
    """Wait until Debezium has flushed an offset at or past the newest row event in Bronze.

    Silver can already equal the source while a replay is still on its way, so the
    duplicate counts are taken only after this point. min_wait > offset.flush.interval.ms.
    """
    t0 = time.time()

    def done() -> dict[str, Any] | None:
        o, m = offsets_file_lsn(), row_max_lsn()
        return (
            {"offsets_file_lsn": o, "bronze_row_max_lsn": m, "waited_s": round(time.time() - t0, 1)}
            if time.time() - t0 >= min_wait and o is not None and o >= m
            else None
        )

    return wait_for(done, timeout=timeout, interval=2, what="offsets to pass the newest Bronze event")


def test_04_replay_after_restarts(summary: dict[str, Any]) -> None:
    result: dict[str, Any] = {}

    # (a) receiver restart in the middle of a 400-transaction workload
    d0 = dup_count()
    settle()
    d0 = dup_count()
    proc = subprocess.Popen(
        ["./run.sh", "workload", "400", "0.33"], cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(1.5)
    sh("./run.sh", "receiver-restart")
    assert proc.wait(timeout=300) == 0
    settle()
    res = converge()
    result["a_receiver_restart"] = {
        "new_duplicates": dup_count() - d0,
        "silver_equals_source": all(d["equal"] for d in res["diffs"].values()),
    }

    # (b) Debezium is killed after the receiver committed a batch but before it got the ack
    d0 = dup_count()
    start_lsn = current_wal_lsn()
    receiver("/admin/fault", {"delay_acks": 100, "delay_s": 15})
    workload(60, 0.44)
    wait_for(lambda: bronze_caught_up(start_lsn + 1), timeout=60, what="a committed, unacked batch in Bronze")
    at_kill: dict[str, Any] = {
        "offsets_file_lsn": offsets_file_lsn(),
        "slot_confirmed_flush_lsn": slot_confirmed_lsn(),
        "bronze_max_lsn": bronze()["source_lsn"].max(),
    }
    sh("docker", "kill", "--signal", "KILL", "cdc-poc-debezium")
    receiver("/admin/fault", {"delay_acks": 0, "delay_s": 0})
    compose("up", "-d", "debezium")
    settled = settle()
    res = converge()
    result["b_kill_after_write_before_ack"] = {
        **at_kill,
        "settled": settled,
        "new_duplicates": dup_count() - d0,
        "silver_equals_source": all(d["equal"] for d in res["diffs"].values()),
    }

    # (c) the sink refuses every write, then Debezium is killed: nothing may be skipped
    start_lsn = current_wal_lsn()
    receiver("/admin/fault", {"reject": True})
    workload(40, 0.45)
    time.sleep(8)
    during: dict[str, Any] = {
        "slot_confirmed_flush_lsn": slot_confirmed_lsn(),
        "offsets_file_lsn": offsets_file_lsn(),
        "wal_lsn_before_workload": start_lsn,
        "receiver_rejected": receiver("/stats")["rejected"],
    }
    sh("docker", "kill", "--signal", "KILL", "cdc-poc-debezium")
    receiver("/admin/fault", {"reject": False})
    compose("up", "-d", "debezium")
    settle()
    res = converge()
    new_rows = bronze().filter(
        pl.col("operation").is_in(["create", "update", "delete"]) & (pl.col("source_lsn") > start_lsn)
    )
    result["c_kill_while_sink_refuses"] = {
        **during,
        "min_lsn_of_refused_events": lsn_min(new_rows["source_lsn"]),
        "refused_events_delivered_after_restart": new_rows.height,
        "slot_stayed_behind_unacked_events": during["slot_confirmed_flush_lsn"] < lsn_min(new_rows["source_lsn"]),
        "silver_equals_source": all(d["equal"] for d in res["diffs"].values()),
    }
    result["bronze_duplicates_total"] = dup_count()
    result["bronze_rows_total"] = bronze().height
    summary["04_restarts"] = result
    write_result("04_restarts", result)
    assert result["a_receiver_restart"]["silver_equals_source"]
    assert result["b_kill_after_write_before_ack"]["new_duplicates"] > 0
    assert result["b_kill_after_write_before_ack"]["silver_equals_source"]
    assert during["receiver_rejected"] > 0
    assert result["c_kill_while_sink_refuses"]["slot_stayed_behind_unacked_events"]
    assert result["c_kill_while_sink_refuses"]["refused_events_delivered_after_restart"] > 0
    assert result["c_kill_while_sink_refuses"]["silver_equals_source"]


# (3) out of order: a late event with a lower LSN lands in Bronze after the newer one
def test_05_late_lower_lsn_event_is_ignored(summary: dict[str, Any]) -> None:
    converge()
    cust = read_silver("customers").sort("_lsn", descending=True)
    victim = cust.row(0, named=True)
    deleted = delete_markers("customers")
    live_ids = set(cust["id"].to_list())
    gone = deleted.filter(~pl.col("id").is_in(list(live_ids))).sort("_lsn", descending=True).row(0, named=True)

    after_live = {k: v for k, v in victim.items() if not k.startswith("_")} | {"name": "STALE LATE UPDATE"}
    late_update = {
        "op": "u",
        "before": None,
        "after": after_live,
        "ts_ms": int(time.time() * 1000),
        "source": {
            "schema": "public",
            "table": "customers",
            "lsn": victim["_lsn"] - 1,
            "sequence": "[null,null]",
            "txId": 1,
            "snapshot": "false",
            "ts_ms": 0,
        },
    }
    after_gone = {
        "id": gone["id"],
        "name": "STALE LATE INSERT",
        "email": "late@example.com",
        "tier": "basic",
        "updated_at": "2026-01-01T00:00:00Z",
    }
    late_insert = {
        "op": "c",
        "before": None,
        "after": after_gone,
        "ts_ms": int(time.time() * 1000),
        "source": {
            "schema": "public",
            "table": "customers",
            "lsn": gone["_lsn"] - 1,
            "sequence": "[null,null]",
            "txId": 1,
            "snapshot": "false",
            "ts_ms": 0,
        },
    }
    append_bronze(envelope_rows([late_update, late_insert], "injected-late"))

    stats = silver_run()
    diffs = {t: diff_silver_vs_source(t, read_silver(t)) for t in TABLES}
    cust_after = read_silver("customers")

    # Control: what an arrival-order ("last ingested wins") apply would produce.
    df = bronze().filter(
        (pl.col("destination") == "cdc.public.customers")
        & pl.col("operation").is_in(["create", "read", "update", "delete"])
    )
    df = df.with_columns(
        pl.coalesce(pl.col("after").str.json_path_match("$.id"), pl.col("before").str.json_path_match("$.id"))
        .cast(pl.Int64)
        .alias("id")
    )
    naive = df.sort("ingest_ts").group_by("id", maintain_order=True).last()
    naive_live = naive.filter(pl.col("operation") != "delete")
    names = [json.loads(a)["name"] for a in naive_live["after"].to_list()]
    naive_names = dict(zip(naive_live["id"].to_list(), names, strict=True))

    # Rebuild Silver from all of Bronze and compare again.
    silver_run(full_refresh=True)
    rebuilt = {t: diff_silver_vs_source(t, read_silver(t)) for t in TABLES}

    result: dict[str, Any] = {
        "late_update": {"id": victim["id"], "silver_lsn": victim["_lsn"], "injected_lsn": victim["_lsn"] - 1},
        "late_insert_for_deleted_key": {"id": gone["id"], "delete_lsn": gone["_lsn"], "injected_lsn": gone["_lsn"] - 1},
        "silver_stats": stats,
        "silver_equals_source": all(d["equal"] for d in diffs.values()),
        "stale_name_in_silver": cust_after.filter(pl.col("name").str.starts_with("STALE")).height,
        "arrival_order_control": {
            "victim_name": naive_names.get(victim["id"]),
            "deleted_key_resurrected": gone["id"] in naive_names,
        },
        "full_refresh_rebuild_equals_source": all(d["equal"] for d in rebuilt.values()),
    }
    summary["05_out_of_order"] = result
    write_result("05_out_of_order", result)
    assert stats["ignored_late"]["customers"] == 2
    assert result["silver_equals_source"]
    assert result["stale_name_in_silver"] == 0
    assert result["arrival_order_control"]["victim_name"] == "STALE LATE UPDATE"
    assert result["arrival_order_control"]["deleted_key_resurrected"]
    assert result["full_refresh_rebuild_equals_source"]


# (4) schema change: ADD COLUMN on the source, envelope Bronze keeps working
def test_06_schema_change_add_column(summary: dict[str, Any]) -> None:
    psql("ALTER TABLE public.customers ADD COLUMN region text")
    psql(
        "UPDATE public.customers SET region = CASE WHEN id % 2 = 0 THEN 'EU' ELSE 'US' END, "
        "updated_at = clock_timestamp() WHERE id % 3 = 0"
    )
    psql(
        "INSERT INTO public.customers (id, name, email, region) "
        "SELECT max(id) + 1, 'with region', 'r@example.com', 'APAC' FROM public.customers"
    )
    workload(60, 0.55)
    res = converge()
    df = bronze().filter(pl.col("destination") == "cdc.public.customers")
    with_region = df.filter(pl.col("after").str.contains('"region"'))
    running = compose("ps", "--status", "running", "--services")
    result: dict[str, Any] = {
        "bronze_customer_events_with_region": with_region.height,
        "bronze_schema_unchanged": bronze().columns,
        "silver_customers_columns": read_silver("customers").columns,
        "silver_rows_with_region": read_silver("customers").filter(pl.col("region").is_not_null()).height,
        "source_rows_with_region": len([r for r in source_rows("customers") if r.get("region")]),
        "silver_equals_source": all(d["equal"] for d in res["diffs"].values()),
        "debezium_running": "debezium" in running,
    }
    summary["06_schema_change"] = result
    write_result("06_schema_change", result)
    assert with_region.height > 0
    assert result["debezium_running"]
    assert result["silver_rows_with_region"] == result["source_rows_with_region"] > 0


# (5) replication slot: WAL is retained while Debezium is stopped, released after it catches up
def test_07_slot_retains_wal_while_stopped(summary: dict[str, Any]) -> None:
    q = (
        "SELECT json_build_object('active', active, 'restart_lsn', restart_lsn::text, "
        "'confirmed_flush_lsn', confirmed_flush_lsn::text, 'wal_status', wal_status, "
        "'retained_bytes', pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, "
        "'pg_wal_dir_bytes', (SELECT sum(size) FROM pg_ls_waldir())::bigint) "
        "FROM pg_replication_slots WHERE slot_name = 'dbz_slot'"
    )

    def slot() -> dict[str, Any]:
        return dict(json.loads(psql(q)))

    running = slot()
    compose("stop", "debezium")
    time.sleep(2)
    stopped_start = slot()
    workload(40, 0.66)  # captured changes made while the reader is down
    psql("INSERT INTO public.noise (payload) SELECT repeat(md5(g::text), 20) FROM generate_series(1, 60000) g")
    psql("CHECKPOINT")
    stopped_end = slot()

    compose("start", "debezium")
    res = converge(timeout=240)
    workload(10, 0.77)
    converge()

    def released() -> dict[str, Any] | None:
        # restart_lsn only moves when a checkpoint's running-xacts record falls behind what Debezium
        # has confirmed, so keep nudging: a checkpoint, then one captured change to confirm past it.
        psql("CHECKPOINT")
        workload(1, 0.78)
        s = slot()
        return s if s["retained_bytes"] < stopped_end["retained_bytes"] // 4 else None

    try:
        after = wait_for(released, timeout=150, interval=6, what="slot to advance")
    except TimeoutError:
        after = slot()

    # Idle captured tables, busy uncaptured table: does the slot still move? (heartbeat.interval.ms only)
    idle_start = slot()
    psql("INSERT INTO public.noise (payload) SELECT repeat(md5(g::text), 20) FROM generate_series(1, 30000) g")
    time.sleep(45)
    idle_end = slot()

    result: dict[str, Any] = {
        "max_slot_wal_keep_size": psql("SHOW max_slot_wal_keep_size"),
        "while_running": running,
        "stopped_start": stopped_start,
        "stopped_end": stopped_end,
        "retained_growth_bytes": stopped_end["retained_bytes"] - stopped_start["retained_bytes"],
        "after_restart_and_catch_up": after,
        "silver_equals_source_after_restart": all(d["equal"] for d in res["diffs"].values()),
        "idle_captured_tables_noise_only": {
            "before": idle_start,
            "after_45s": idle_end,
            "confirmed_flush_moved": idle_start["confirmed_flush_lsn"] != idle_end["confirmed_flush_lsn"],
        },
    }
    summary["07_slot_wal"] = result
    write_result("07_slot_wal", result)
    assert stopped_start["active"] is False
    assert result["retained_growth_bytes"] > 20 * 1024 * 1024
    assert after["retained_bytes"] < stopped_end["retained_bytes"]


# (5) the verify command: a full row-level diff of Postgres against Silver, exit code 0 or 1
def test_08_verify_command_detects_drift(summary: dict[str, Any]) -> None:
    converge()
    code_ok, out_ok = run_cli("./run.sh", "verify")

    # One source change that Silver has not seen yet (the Silver job has not run) must fail the check.
    psql("UPDATE public.customers SET name = 'VERIFY PROBE' WHERE id = (SELECT min(id) FROM public.customers)")
    code_drift, out_drift = run_cli("./run.sh", "verify")

    converge()
    code_after, out_after = run_cli("./run.sh", "verify")

    result: dict[str, Any] = {
        "exit_code_when_converged": code_ok,
        "exit_code_with_unapplied_change": code_drift,
        "exit_code_after_silver_run": code_after,
        "drift_report_names_the_row": "VERIFY PROBE" in out_drift and "only in Postgres" in out_drift,
        "converged_output": out_ok.strip().splitlines()[-3:],
        "drift_output": out_drift.strip().splitlines()[:6],
    }
    summary["08_verify"] = result
    write_result("08_verify", result)
    assert (code_ok, code_drift, code_after) == (0, 1, 0), (out_ok, out_drift, out_after)
    assert result["drift_report_names_the_row"], out_drift
