#!/usr/bin/env python3
"""Monthly Kronos retrain orchestrator."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from tools.pipeline.run_model_qa import run_evaluation  # noqa: E402


def _resolve_latest(model_dir: Path) -> Optional[Path]:
    link = model_dir / "latest"
    if not link.exists():
        return None
    target = link.resolve()
    return target if target.parent == model_dir else target


def _update_latest_symlink(model_dir: Path, target: Path) -> None:
    link = model_dir / "latest"
    if link.exists() or link.is_symlink():
        link.unlink()
    link.symlink_to(target.name)


def _parse_offsets(raw: str) -> List[int]:
    if not raw:
        return [1, 7, 30]
    return [int(token) for token in raw.split(",") if token.strip()]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monthly Kronos retrain + QA promotion helper.")
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=Path("/mnt/data/models/kronos-nbeats"),
        help="Directory containing Kronos model artifacts.",
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=150,
        help="Number of symbols sampled during QA evaluation.",
    )
    parser.add_argument(
        "--offsets",
        type=str,
        default="1,7,30",
        help="Comma-separated day offsets for QA checkpoints.",
    )
    parser.add_argument(
        "--train-extra-args",
        nargs=argparse.REMAINDER,
        help="Optional extra args passed to tools/kronos_data/train_nbeats.py",
    )
    parser.add_argument(
        "--qa-output",
        type=Path,
        help="Optional path to store the QA JSON report.",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    model_dir = args.model_dir.resolve()
    model_dir.mkdir(parents=True, exist_ok=True)

    previous_artifact = _resolve_latest(model_dir)
    offsets = _parse_offsets(args.offsets)
    print(f"[{datetime.utcnow().isoformat()}Z] Starting Kronos retrain in {model_dir}")

    train_cmd = [sys.executable, "tools/kronos_data/train_nbeats.py", "--out", str(model_dir)]
    if args.train_extra_args:
        train_cmd.extend(args.train_extra_args)
    subprocess.run(train_cmd, cwd=ROOT_DIR, check=True)

    latest_artifact = _resolve_latest(model_dir)
    if latest_artifact is None:
        raise SystemExit("Training did not produce a latest artifact.")

    artifact_paths = [latest_artifact]
    if previous_artifact and previous_artifact != latest_artifact:
        artifact_paths.append(previous_artifact)
    report = run_evaluation(
        artifact_paths,
        as_of_dates=[date.today() - timedelta(days=offset) for offset in offsets],
        max_symbols=args.max_symbols,
    )

    qa_path = args.qa_output or (model_dir / f"qa_report_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.json")
    qa_path.parent.mkdir(parents=True, exist_ok=True)
    qa_path.write_text(json.dumps(report, indent=2))
    print(f"Wrote QA report to {qa_path}")

    winner_path = Path(report.get("winner") or latest_artifact)
    if previous_artifact and winner_path.resolve() == previous_artifact.resolve():
        print("QA indicates previous artifact performs better. Reverting latest symlink.")
        _update_latest_symlink(model_dir, previous_artifact)
    else:
        print(f"Promoting {latest_artifact} as the active artifact.")


if __name__ == "__main__":
    main()
