from __future__ import annotations

import argparse
import subprocess
import sys
from typing import Dict, List

PRESET_MODULES: Dict[str, str] = {
    "signal-universe": "tools.universe.build_signal_universe",
    "daily-plans": "tools.planner.build_daily_plans",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="PulseTrade tools runner",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "task",
        choices=sorted(PRESET_MODULES),
        help="Which preset tooling task to execute.",
    )
    parser.add_argument(
        "task_args",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded to the underlying module (prefix with --).",
    )
    return parser


def run_task(task: str, extra_args: List[str]) -> int:
    module = PRESET_MODULES[task]
    passthrough = list(extra_args)
    if passthrough and passthrough[0] == "--":
        passthrough = passthrough[1:]
    cmd = [sys.executable, "-m", module]
    if passthrough:
        cmd.extend(passthrough)
    print(f"[tools.run] executing: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=False)
    return result.returncode


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return run_task(args.task, args.task_args)


if __name__ == "__main__":
    raise SystemExit(main())
