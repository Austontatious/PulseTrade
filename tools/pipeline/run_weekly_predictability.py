#!/usr/bin/env python3
"""Weekly predictability pipeline.

This CLI refreshes the "most predictable 100" universe by:
  1. Pulling candidate symbols from alpaca_universe joined with coverage_180_final.
  2. Computing predictability metrics from recent Kronos forecasts vs realized returns.
  3. Ranking symbols by 1 / (1e-6 + MAE) and persisting the top N into signal_universe_100.
  4. Writing reports/predictability_snapshot_<as_of>.json for downstream LLM processing.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

import psycopg2

from tools.universe.build_signal_universe import (  # noqa: E402
    build_predictability_universe,
    database_url,
)

DEFAULT_TOP_K = int(os.getenv("PREDICTABILITY_TOPK", "100"))
DEFAULT_HORIZON = os.getenv("SIGNAL_UNIVERSE_FORECAST_HORIZON", os.getenv("FORECAST_HORIZON", "5d"))
REPORTS_DIR = ROOT_DIR / "reports"
logger = logging.getLogger("predictability")


def _parse_date(value: str | None) -> date:
    if not value:
        return date.today()
    return date.fromisoformat(value)


def _parse_horizon_days(value: str) -> int:
    if not value:
        return 5
    digits = "".join(ch for ch in value if ch.isdigit())
    try:
        return int(digits or value)
    except ValueError:
        raise SystemExit(f"Invalid horizon value: {value}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build weekly 'most predictable' universe snapshot.")
    parser.add_argument("--as-of", type=str, required=True, help="Snapshot date (YYYY-MM-DD).")
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K, help="How many symbols to persist (default: 100).")
    parser.add_argument(
        "--eval-days",
        type=int,
        default=int(os.getenv("PREDICTABILITY_LOOKBACK_DAYS", "120")),
        help="Lookback window (days) for evaluating forecasts vs realized returns.",
    )
    parser.add_argument(
        "--min-pairs",
        type=int,
        default=int(os.getenv("PREDICTABILITY_MIN_PAIRS", "60")),
        help="Minimum forecast/realized pairs required to score a symbol.",
    )
    parser.add_argument(
        "--horizon",
        type=str,
        default=DEFAULT_HORIZON,
        help="Forecast horizon to evaluate (default: 5d).",
    )
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        default=REPORTS_DIR,
        help=f"Directory for snapshot JSON (default: {REPORTS_DIR}).",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = parse_args()
    as_of = _parse_date(args.as_of)
    snapshot_dir = args.snapshot_dir
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    horizon_days = _parse_horizon_days(args.horizon)
    logger.info(
        "Building predictability universe for %s (top_k=%s, horizon_days=%s, eval_days=%s, min_pairs=%s)",
        as_of,
        args.top_k,
        horizon_days,
        args.eval_days,
        args.min_pairs,
    )
    with psycopg2.connect(database_url()) as conn:
        count = build_predictability_universe(
            conn,
            as_of=as_of,
            top_k=args.top_k,
            eval_days=args.eval_days,
            min_pairs=args.min_pairs,
            horizon_days=horizon_days,
            snapshot_dir=snapshot_dir,
        )
        if count <= 0:
            logger.warning("No predictability metrics computed; skipping persistence.")
        else:
            logger.info("Persisted %d predictability rows into signal_universe_100", count)


if __name__ == "__main__":
    main()
