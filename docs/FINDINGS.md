# Findings

What the integration run showed, what it means for a push-based CDC design, and what it does not cover.
Exact counts for the latest run are in the README results table and in `results/`. Values that vary with timing
(duplicate counts, LSNs, row counts) are not repeated here.

## Confirmed

- **Snapshot rows carry a non-null `source.lsn`.** Every snapshot row had the same LSN, the slot's consistent point,
  and every later change had a higher one. Ordering by `source.lsn` has no NULLs to reject, and the snapshot row
  for a key always loses to a later change (Debezium 3.7.1, Postgres 17).
- **Delivery is at-least-once.** Duplicates appeared after a lost ack and after a crash between write and ack,
  with the same `idempotency_key`. Removing duplicates on that key was enough to keep Silver correct.
- **The durable ack protects against loss.** While the sink refused writes, neither the offsets file nor the slot
  moved past the unacknowledged events, and a crash in that state lost nothing.
- **Order by LSN, not by arrival.** The arrival-order control in `test_05` produced a wrong Silver table: it took
  the stale value and brought back a deleted row.
- **Deletes have only a `before` image**, so the key comes from `before`.
- **A slot holds WAL while nobody reads it**, with no cap by default (`max_slot_wal_keep_size = -1`).
- **`restart_lsn` can lag `confirmed_flush_lsn`.** After Debezium caught up, WAL was released only after a
  checkpoint plus one more captured change (the integration test nudges both). In an idle database, retained WAL
  can stay high even though Debezium is current, so alert on slot lag as well as on consumer lag.
- **The heartbeat interval alone did not keep the slot moving** when only uncaptured tables were busy. This
  supports adding `heartbeat.action.query`. Debezium 3.7.1 logs the same hint at startup.
- **An envelope Bronze table keeps working through `ADD COLUMN`**, and Silver can be rebuilt from Bronze at any
  time with `./run.sh silver --full-refresh`.

## Refined or new

- The slot's `confirmed_flush_lsn` matched the offsets file exactly at each kill. The replay window after a crash
  is set by `offset.flush.interval.ms` (20 s here). Keep the offsets file on durable storage and set the interval
  with the replay cost in mind.
- `source.sequence` on snapshot rows is `[null, lsn]`: the `lastCommitLsn` part is null. A tie-breaker built from
  `sequence` must handle that null first element.
- Debezium 3.7 adds `lsn.flush.mode` (default `connector`) and `offset.mismatch.strategy`. The value
  `connector_and_driver` is another documented way to stop idle WAL growth. Not tested here.

## Run history

Earlier runs of the same tests agreed on every deterministic value (150 snapshot reads, 0 NULL LSNs, Silver equals
source in every scenario) and differed in timing-dependent counts. Two test races were fixed along the way, neither
a product bug: the 5 s heartbeat batches could use up the injected lost-ack faults (ack faults now apply only to
batches that carry row events), and the slot-release wait now nudges with a checkpoint and a captured change.

## How this maps to a managed ingest path

| This POC | Managed path |
| --- | --- |
| Debezium Server HTTP sink, batch mode | Debezium Server Zerobus Ingest sink (`debezium.sink.type=zerobus`) |
| Receiver writing Delta with delta-rs | Zerobus Ingest writing Unity Catalog Delta tables |
| 2xx after the Delta commit | Durable acknowledgment before the offset advances |
| JSON string columns `before`, `after`, `value` | `VARIANT` columns in envelope mode |
| Our `idempotency_key` | The sink's `idempotency_key` |
| Silver job in `src/cdc_poc/silver/` | A Lakeflow pipeline with a cleaning streaming table plus `AUTO CDC ... SEQUENCE BY source_lsn ... STORED AS SCD TYPE 1` |
| Delete-marker handling in `LateEventFilter` | AUTO CDC's own handling of deletes and late events |

## Not covered

- Zerobus itself: OAuth, gRPC streams, quotas, the unflushed-data limit, error 4044 in typed mode.
- Unity Catalog, Spark streaming dedupe and watermarks, the integrated Postgres CDC pipeline.
- Tombstone rows. The Debezium HTTP sink skips records with a null value, so tombstones never reach the receiver
  (0 in Bronze). Whether a managed sink writes `operation = 'tombstone'` rows has to be checked against that
  sink's documentation.
- TOAST values, tables without a primary key, multiple consumers or slots, a Postgres crash (as opposed to a
  Debezium crash), and anything at production volume. The receiver is single-writer and does one Delta commit per
  HTTP batch, with no compaction.

## Notes

- The receiver runs on the host with uv instead of in a container. This avoids building an image and keeps the
  Docker disk use small. Debezium reaches it at `host.docker.internal:18080`.
- `./run.sh test` leaves the containers stopped. The Postgres volume and `./data` stay for inspection;
  `./run.sh reset` removes them.
