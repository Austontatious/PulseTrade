#!/usr/bin/env python3
"""Generate nightly Top-100 forecasts for the next trading days."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SYMBOLS = ROOT / "services/ingest/universe_symbols.txt"
DEFAULT_MODEL_DIR = Path("/mnt/data/models/kronos-nbeats/latest")
DEFAULT_DATA = Path("/mnt/data/kronos_data/processed/nbeats_alpaca_daily.parquet")
DEFAULT_OUT_DIR = ROOT / "reports"
PREDICT_SCRIPT = ROOT / "tools/kronos_data/predict_top100_next_day.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run nightly Top-100 forecasts for upcoming days.")
    parser.add_argument("--as-of", required=True, help="Use data up to and including this date (YYYY-MM-DD).")
    parser.add_argument(
        "--predict-horizons",
        default="1,2",
        help="Comma-separated day offsets to export (default: 1,2).",
    )
    parser.add_argument("--symbols-file", type=Path, default=DEFAULT_SYMBOLS)
    parser.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument(
        "--extra-args",
        nargs=argparse.REMAINDER,
        help="Additional arguments passed through to predict_top100_next_day.py",
    )
    return parser.parse_args()


def run(args: argparse.Namespace) -> None:
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"top100_predictions_{args.as_of}.json"

    cmd: List[str] = [
        sys.executable,
        str(PREDICT_SCRIPT),
        "--as-of",
        args.as_of,
        "--symbols-file",
        str(args.symbols_file),
        "--model-dir",
        str(args.model_dir),
        "--data",
        str(args.data),
        "--predict-horizons",
        args.predict_horizons,
        "--out",
        str(output_path),
    ]
    if args.extra_args:
        cmd.extend(args.extra_args)
    print(f"[nightly_forecasts] running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    print(f"[nightly_forecasts] forecasts written to {output_path}")


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
