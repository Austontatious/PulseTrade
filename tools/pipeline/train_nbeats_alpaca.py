"""End-to-end pipeline: build dataset, train NBEATS, rank top symbols."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATASET = Path("/mnt/data/kronos_data/processed/nbeats_alpaca_daily.parquet")
DEFAULT_RESIDUALS = Path("/mnt/data/kronos_data/processed/nbeats_alpaca_residuals.parquet")
DEFAULT_REPORT = Path("/mnt/data/PulseTrade/reports/top100_best_fit.json")
DEFAULT_UNIVERSE = Path("/mnt/data/PulseTrade/services/ingest/universe_symbols.txt")


def run(cmd: list[str]) -> None:
    print(f"[pipeline] running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train NBEATS on Alpaca universe and select Top-K symbols.")
    parser.add_argument("--symbols-file", type=Path, default=Path("/mnt/data/PulseTrade/db/alpaca_universe.symbols.txt"))
    parser.add_argument("--lookback-days", type=int, default=190)
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--model-out", type=Path, default=Path("/mnt/data/models/kronos-nbeats"))
    parser.add_argument("--feature-cols", type=str, default="")
    parser.add_argument("--horizon", type=int, default=5)
    parser.add_argument("--input-size", type=int, default=126)
    parser.add_argument("--max-epochs", type=int, default=80)
    parser.add_argument("--top-k", type=int, default=100)
    parser.add_argument("--residuals", type=Path, default=DEFAULT_RESIDUALS)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--update-universe", type=Path, default=None)
    parser.add_argument("--min-holdout-days", type=int, default=365)
    parser.add_argument("--min-holdout-frac", type=float, default=0.2)
    parser.add_argument("--devices", type=str, default="1")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Step 1: build dataset
    run(
        [
            sys.executable,
            str(REPO_ROOT / "tools/kronos_data/build_alpaca_training_set.py"),
            "--symbols",
            str(args.symbols_file),
            "--lookback-days",
            str(args.lookback_days),
            "--out",
            str(args.dataset),
        ]
    )

    # Step 2: train NBEATS
    train_cmd = [
        sys.executable,
        str(REPO_ROOT / "tools/kronos_data/train_nbeats.py"),
        "--data",
        str(args.dataset),
        "--out",
        str(args.model_out),
        "--horizon",
        str(args.horizon),
        "--input-size",
        str(args.input_size),
        "--max-epochs",
        str(args.max_epochs),
        "--residuals-out",
        str(args.residuals),
        "--min-holdout-days",
        str(args.min_holdout_days),
        "--min-holdout-frac",
        str(args.min_holdout_frac),
        "--devices",
        args.devices,
    ]
    if args.feature_cols:
        train_cmd.extend(["--feature-cols", args.feature_cols])
    run(train_cmd)

    # Step 3: rank best-fit symbols
    rank_cmd = [
        sys.executable,
        str(REPO_ROOT / "tools/kronos_data/rank_best_fit.py"),
        "--residuals",
        str(args.residuals),
        "--top-k",
        str(args.top_k),
        "--report",
        str(args.report),
    ]
    if args.update_universe:
        rank_cmd.extend(["--update-file", str(args.update_universe)])
    run(rank_cmd)


if __name__ == "__main__":
    main()
