#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/mnt/data/PulseTrade"
LOG_DIR="/mnt/data/logs/strategy_eval"
LOG_FILE="$LOG_DIR/nightly.log"
DOCKER_BIN="/usr/bin/docker"

mkdir -p "$LOG_DIR"
cd "$PROJECT_ROOT"

timestamp() {
  date -Is
}

echo "[$(timestamp)] nightly strategy eval: start" >> "$LOG_FILE"

set +e
$DOCKER_BIN compose --ansi never run --rm tools bash -lc 'cd /workspace && PYTHONPATH=/workspace python tools/kronos_eval/daily_strategy_eval.py'
status=$?
set -e

if [[ $status -eq 0 ]]; then
  echo "[$(timestamp)] nightly strategy eval: completed ok" >> "$LOG_FILE"
else
  echo "[$(timestamp)] nightly strategy eval: FAILED with status $status" >> "$LOG_FILE"
  exit $status
fi
