"""Offline evaluation harness for LLM prompts."""

from __future__ import annotations

import json
import os
import pathlib
import time

import pandas as pd

from libs.llm.run_and_log import LLMRunner
from libs.llm.schemas import POLICY_SCHEMA


DATA_PATH = os.getenv("LLM_EVAL_DATA", "data/historical_signals.parquet")
RESULT_DIR = pathlib.Path("results")


def main() -> None:
    runner = LLMRunner()
    df = pd.read_parquet(DATA_PATH)
    rows: list[dict[str, object]] = []

    for _, record in df.iterrows():
        tpl = {
            "ticker": record.symbol,
            "as_of": str(record.date),
            "inputs_json": json.dumps(
                {
                    "earnings_date": str(record.earnings_date),
                    "liquidity_usd": float(record.liquidity),
                    "borrow_ok": bool(record.borrow_ok),
                    "factor_dispersion": float(record.factor_dispersion),
                    "coverage_pct": float(record.coverage_pct),
                    "macro_regime": record.macro_regime,
                    "signal_label": record.signal_label,
                    "signal_value": float(record.signal_value),
                    "min_liquidity": float(os.getenv("LLM_POLICY_MIN_LIQUIDITY", 2_000_000)),
                }
            ),
        }
        started = time.time()
        result = runner.cached_call("policy", tpl, schema=POLICY_SCHEMA)
        rows.append(
            {
                "symbol": record.symbol,
                "date": record.date,
                "ok": bool(result.get("success", True)),
                "latency_ms": int((time.time() - started) * 1000),
                "allow": None if not result.get("json") else result["json"].get("allow"),
                "cached": result.get("cached", False),
            }
        )

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RESULT_DIR / f"llm_eval_{int(time.time())}.parquet"
    pd.DataFrame(rows).to_parquet(out_path)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
