#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
DATE_ARG="${1:-$(TZ=America/Chicago date +%F)}"
cd "$ROOT_DIR"
echo "[signal-universe] building snapshot for ${DATE_ARG}" >&2
docker compose run --rm tools \
  bash -lc "python tools/universe/build_signal_universe.py --as-of '${DATE_ARG}'"
