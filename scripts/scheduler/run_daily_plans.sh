#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
DATE_ARG="${1:-$(TZ=America/Chicago date +%F)}"
cd "$ROOT_DIR"
echo "[daily-plans] building plans for ${DATE_ARG}" >&2
docker compose run --rm tools \
  bash -lc "python tools/planner/build_daily_plans.py --date '${DATE_ARG}'"
