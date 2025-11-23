#!/usr/bin/env bash
set -euo pipefail

DB_PATH=/data/device_events.duckdb
UI_PORT=4214
DUCKDB_BIN=/usr/local/bin/duckdb

DUCKDB_CMD=("$DUCKDB_BIN" "$DB_PATH" -cmd "SET ui_local_port=$UI_PORT; CALL start_ui_server();")

cleanup() {
  for pid in "${duckdb_pid:-}" "${socat_pid:-}"; do
    if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}

trap cleanup EXIT INT TERM

"${DUCKDB_CMD[@]}" &
duckdb_pid=$!

socat TCP-LISTEN:4213,reuseaddr,fork TCP:127.0.0.1:$UI_PORT &
socat_pid=$!

wait "$duckdb_pid"
