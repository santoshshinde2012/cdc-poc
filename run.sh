#!/usr/bin/env bash
# Local push-based CDC proof of concept: Postgres -> Debezium Server (HTTP sink) -> receiver -> Bronze Delta -> Silver Delta.
set -euo pipefail
cd "$(dirname "$0")"

PSQL=(docker compose exec -T postgres psql -U app -d shop -v ON_ERROR_STOP=1)

RECEIVER_PID=data/receiver.pid
RECEIVER_PORT=18080

die() { echo "error: $*" >&2; exit 1; }

preflight() {
  command -v docker >/dev/null || die "docker not found. Install Docker Desktop, or Docker Engine with the compose plugin."
  docker info >/dev/null 2>&1 || die "the Docker daemon is not running. Start Docker Desktop and retry."
  docker compose version >/dev/null 2>&1 || die "'docker compose' (v2) is not available. Update Docker."
  command -v uv >/dev/null || die "uv not found. Install it from https://docs.astral.sh/uv/ and retry."
}

need_postgres() {
  preflight
  docker compose ps --status running --services 2>/dev/null | grep -qx postgres \
    || die "postgres is not running. Run ./run.sh up first."
}

receiver_start() {
  mkdir -p data
  if [ -f "$RECEIVER_PID" ] && kill -0 "$(cat "$RECEIVER_PID")" 2>/dev/null; then return; fi
  holder=$(lsof -tiTCP:$RECEIVER_PORT -sTCP:LISTEN 2>/dev/null || true)
  [ -z "$holder" ] || die "port $RECEIVER_PORT is already in use (pid $holder). Stop that process, or run ./run.sh down to stop an old receiver."
  BRONZE_PATH="$PWD/data/bronze/events" PORT=$RECEIVER_PORT \
    nohup uv run cdc-receiver >> data/receiver.log 2>&1 &
  echo $! > "$RECEIVER_PID"
  for _ in $(seq 1 60); do
    curl -sf http://localhost:$RECEIVER_PORT/health >/dev/null && { echo "receiver up (pid $(cat "$RECEIVER_PID"))"; return; }
    sleep 0.5
  done
  echo "--- last lines of data/receiver.log ---" >&2; tail -n 15 data/receiver.log >&2 || true
  die "the receiver did not start within 30 s."
}

receiver_stop() {
  if [ -f "$RECEIVER_PID" ]; then
    pid=$(cat "$RECEIVER_PID")
    # SIGTERM: the receiver finishes the batch it is committing, then exits. Kill only after 15 s.
    pkill -TERM -P "$pid" 2>/dev/null || true
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    if kill -0 "$pid" 2>/dev/null; then
      echo "receiver did not stop in 15 s, killing it" >&2
      pkill -KILL -P "$pid" 2>/dev/null || true; kill -KILL "$pid" 2>/dev/null || true
    fi
    rm -f "$RECEIVER_PID"
  fi
  # uv run spawns a child; make sure nothing still holds the port
  pids=$(lsof -tiTCP:$RECEIVER_PORT -sTCP:LISTEN 2>/dev/null || true)
  [ -z "$pids" ] || kill $pids 2>/dev/null || true
}

step() { printf '\n==> %s\n' "$*"; }

# Run the Silver job, then verify, until Silver matches Postgres or the time limit passes.
converge() {
  local limit="${1:-180}" start=$SECONDS
  while :; do
    uv run cdc-silver >/dev/null 2>&1 || true
    uv run cdc-verify >/dev/null 2>&1 && return 0
    (( SECONDS - start >= limit )) && return 1
    printf '.'; sleep 3
  done
}

usage() {
  cat <<'U'
usage: ./run.sh <command>
  up         start postgres (docker), the receiver (uv, on the host) and debezium (docker)
  receiver-restart  stop and start the receiver
  seed       load the workload procedure into postgres
  workload   run N mixed transactions (default 200): ./run.sh workload 500
  silver     apply new Bronze events to Silver (add --full-refresh to rebuild from all Bronze)
  verify     row-by-row diff of Postgres against Silver, exit 1 on any mismatch (run silver first)
  status     slot and retained WAL, source counts, Bronze counts, Silver counts
  demo       reset, up, seed, workload, silver, verify, status in one go: ./run.sh demo [transactions]
  unit       fast unit tests, no Docker (in-memory adapters)
  check      ruff lint, ruff format check, mypy strict
  integration  reset the stack and run the 8 end-to-end tests (needs Docker)
  test       check + unit + integration (output in results/)
  logs       follow debezium and receiver logs
  down       stop containers (keeps the postgres volume and ./data)
  reset      stop containers, drop the postgres volume and delete ./data
U
}

cmd="${1:-}"; shift || true
case "$cmd" in
  up)
    preflight
    mkdir -p data/debezium data/bronze
    docker compose up -d --wait postgres \
      || die "postgres did not become healthy. Check 'docker compose logs postgres'. If the port is taken, set POSTGRES_PORT (default 55432)."
    receiver_start
    docker compose up -d --wait debezium \
      || { docker compose logs --tail 30 debezium >&2; die "debezium did not become healthy, see the log above."; }
    echo "up. Bronze: data/bronze/events  Debezium offsets: data/debezium/offsets.dat"
    ;;
  receiver-restart)
    receiver_stop
    receiver_start
    ;;
  seed)
    need_postgres
    "${PSQL[@]}" -f /sql/workload.sql
    ;;
  workload)
    need_postgres
    n="${1:-200}"; seed="${2:-0.42}"
    [[ "$n" =~ ^[0-9]+$ ]] || die "workload takes a number of transactions, got '$n'. Example: ./run.sh workload 500"
    "${PSQL[@]}" -f /sql/workload.sql >/dev/null
    "${PSQL[@]}" -c "CALL run_workload($n, $seed);"
    ;;
  silver)
    command -v uv >/dev/null || die "uv not found. Install it from https://docs.astral.sh/uv/"
    [ -d data/bronze/events/_delta_log ] || die "no Bronze table yet. Run ./run.sh up and give Debezium a few seconds to snapshot."
    CDC_DATA="$PWD/data" uv run cdc-silver "$@"
    ;;
  verify)
    need_postgres
    CDC_DATA="$PWD/data" uv run cdc-verify "$@"
    ;;
  demo)
    n="${1:-200}"
    [[ "$n" =~ ^[0-9]+$ ]] || die "demo takes a number of transactions, got '$n'"
    preflight
    t0=$SECONDS
    step "1/7 reset: start from a clean state"; "$0" reset
    step "2/7 up: postgres, receiver, debezium (waits for healthchecks)"; "$0" up
    step "3/7 seed: load the workload procedure"; "$0" seed >/dev/null; echo "ok"
    step "4/7 workload: $n mixed transactions"; "$0" workload "$n"
    step "5/7 silver: apply Bronze to Silver until it matches Postgres (up to 180 s)"
    converge 180 && echo " converged" || { echo; die "Silver did not converge within 180 s. Run ./run.sh logs"; }
    step "6/7 verify: compare every row, every column"; "$0" verify
    step "7/7 status"; "$0" status
    printf '\ndemo finished in %d s. The stack is still running: ./run.sh down stops it, ./run.sh reset also clears the data.\n' $((SECONDS - t0))
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
    preflight
    mkdir -p results
    uv run pytest -v tests/integration "$@" 2>&1 | tee results/pytest-output.txt
    exit "${PIPESTATUS[0]}"
    ;;
  status)
    need_postgres
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
    docker compose down -t 30
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
