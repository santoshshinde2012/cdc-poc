#!/usr/bin/env bash
# Local push-based CDC proof of concept: Postgres -> Debezium Server (HTTP sink) -> receiver -> Bronze Delta -> Silver Delta.
set -euo pipefail
cd "$(dirname "$0")"

PSQL=(docker compose exec -T postgres psql -U app -d shop -v ON_ERROR_STOP=1)

RECEIVER_PID=data/receiver.pid

receiver_start() {
  mkdir -p data
  if [ -f "$RECEIVER_PID" ] && kill -0 "$(cat "$RECEIVER_PID")" 2>/dev/null; then return; fi
  BRONZE_PATH="$PWD/data/bronze/events" PORT=18080 \
    nohup uv run cdc-receiver >> data/receiver.log 2>&1 &
  echo $! > "$RECEIVER_PID"
  for _ in $(seq 1 60); do
    curl -sf http://localhost:18080/health >/dev/null && { echo "receiver up (pid $(cat "$RECEIVER_PID"))"; return; }
    sleep 0.5
  done
  echo "receiver did not start, see data/receiver.log" >&2; exit 1
}

receiver_stop() {
  if [ -f "$RECEIVER_PID" ]; then
    pid=$(cat "$RECEIVER_PID")
    pkill -P "$pid" 2>/dev/null || true
    kill "$pid" 2>/dev/null || true
    rm -f "$RECEIVER_PID"
    sleep 0.5
  fi
  # uv run spawns a child; make sure nothing still holds the port
  pids=$(lsof -tiTCP:18080 -sTCP:LISTEN 2>/dev/null || true)
  [ -n "$pids" ] && kill $pids 2>/dev/null || true
}

usage() {
  cat <<'U'
usage: ./run.sh <command>
  up         start postgres (docker), the receiver (uv, on the host) and debezium (docker)
  receiver-restart  stop and start the receiver
  seed       load the workload procedure into postgres
  workload   run N mixed transactions (default 200): ./run.sh workload 500
  silver     apply new Bronze events to Silver (add --full-refresh to rebuild from all Bronze)
  status     slot and retained WAL, source counts, Bronze counts, Silver counts
  unit       fast unit tests, no Docker (in-memory adapters)
  check      ruff lint, ruff format check, mypy strict
  integration  reset the stack and run the 7 end-to-end tests (needs Docker)
  test       check + unit + integration (output in results/)
  logs       follow debezium and receiver logs
  down       stop containers (keeps the postgres volume and ./data)
  reset      stop containers, drop the postgres volume and delete ./data
U
}

cmd="${1:-}"; shift || true
case "$cmd" in
  up)
    mkdir -p data/debezium data/bronze
    docker compose up -d --wait postgres
    receiver_start
    docker compose up -d debezium
    echo "up. Bronze: data/bronze/events  Debezium offsets: data/debezium/offsets.dat"
    ;;
  receiver-restart)
    receiver_stop
    receiver_start
    ;;
  seed)
    "${PSQL[@]}" -f /sql/workload.sql
    ;;
  workload)
    n="${1:-200}"; seed="${2:-0.42}"
    "${PSQL[@]}" -f /sql/workload.sql >/dev/null
    "${PSQL[@]}" -c "CALL run_workload($n, $seed);"
    ;;
  silver)
    CDC_DATA="$PWD/data" uv run cdc-silver "$@"
    ;;
  unit)
    mkdir -p results
    uv run pytest tests/unit -q "$@" 2>&1 | tee results/unit-output.txt
    exit "${PIPESTATUS[0]}"
    ;;
  check)
    mkdir -p results
    { echo "$ ruff check"; uv run ruff check src tests && echo "$ ruff format --check" && uv run ruff format --check src tests \
      && echo "$ mypy" && uv run mypy; } 2>&1 | tee results/check-output.txt
    exit "${PIPESTATUS[0]}"
    ;;
  integration)
    mkdir -p results
    uv run pytest -v tests/integration "$@" 2>&1 | tee results/pytest-output.txt
    exit "${PIPESTATUS[0]}"
    ;;
  status)
    "${PSQL[@]}" -c "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal FROM pg_replication_slots;"
    "${PSQL[@]}" -At -c "SELECT 'source customers='||(SELECT count(*) FROM customers)||' orders='||(SELECT count(*) FROM orders);"
    uv run python -c "
from pathlib import Path
from deltalake import DeltaTable; import polars as pl
df = pl.from_arrow(DeltaTable('data/bronze/events').to_pyarrow_dataset().to_table())
rows = df.filter(pl.col('operation') != 'heartbeat')
print(f'bronze rows {df.height} (heartbeats {df.height - rows.height})')
dups = rows.height - rows['idempotency_key'].n_unique()
print(f'bronze duplicate rows {dups} (same idempotency_key, removed by the Silver job)')
print(df.group_by('destination','operation').len().sort('destination','operation'))
for t in ('customers', 'orders'):
    p = Path('data/silver') / t
    print('silver', t, DeltaTable(str(p)).to_pyarrow_dataset().count_rows() if p.exists() else 'not built yet (run ./run.sh silver)')
"
    ;;
  test)
    "$0" check
    "$0" unit
    "$0" integration "$@"
    ;;
  logs)
    tail -f data/receiver.log & docker compose logs -f debezium
    ;;
  down)
    docker compose down
    receiver_stop
    ;;
  reset)
    docker compose down -v --remove-orphans
    receiver_stop
    rm -rf data
    mkdir -p data
    ;;
  help|-h|--help) usage ;;
  *) usage; exit 1 ;;
esac
