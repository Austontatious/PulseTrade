#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT_DIR"

LIMIT="${1:-${LIQUIDITY_BACKFILL_LIMIT:-300}}"
LOOKBACK="${LIQUIDITY_BACKFILL_LOOKBACK_DAYS:-90}"

echo "[liquidity-backfill] running with limit=${LIMIT} lookback=${LOOKBACK}" >&2
docker compose run --rm tools \
  bash -lc "python scripts/backfill_liquidity.py --limit '${LIMIT}' --lookback-days '${LOOKBACK}'"
