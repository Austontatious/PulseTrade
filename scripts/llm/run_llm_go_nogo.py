#!/usr/bin/env python3
"""Weekly LLM go/no-go evaluation for the top universe symbols."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import os
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

import psycopg2

from libs.llm.reviews import LLMReviewStore
from libs.llm.run_and_log import LLMRunner
from libs.llm.schemas import WEEKLY_GO_NOGO_SCHEMA
from tools.llm_go_nogo import (
    fetch_fundamentals,
    fetch_political_signals,
    fetch_sentiment_snapshot,
    fetch_signal_universe,
    fetch_technical_metrics,
    fetch_time_series_forecast,
    resolve_as_of,
)
from tools.universe import build_signal_universe


REPORTS_DIR = Path("reports")


def _database_url() -> str:
    return build_signal_universe.database_url()


def _build_payload(
    conn,
    *,
    symbol: str,
    as_of: dt.date,
    entry_meta: Dict[str, Any],
    horizon: str,
) -> Dict[str, Any]:
    payload = {
        "symbol": symbol,
        "as_of": as_of.isoformat(),
        "forecast": fetch_time_series_forecast(conn, symbol, horizon),
        "technical": fetch_technical_metrics(conn, symbol, as_of),
        "sentiment": fetch_sentiment_snapshot(conn, symbol),
        "fundamentals": fetch_fundamentals(conn, symbol),
        "political_data": fetch_political_signals(conn, symbol, as_of),
        "meta": entry_meta,
    }
    return payload


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run weekly LLM go/no-go checks for the top universe.")
    parser.add_argument("--as-of", type=lambda s: dt.date.fromisoformat(s), help="Universe snapshot date (defaults to latest)")
    parser.add_argument("--limit", type=int, default=100, help="Number of symbols to evaluate (default 100)")
    parser.add_argument("--horizon", type=str, default="5d", help="Forecast horizon tag to inspect (default 5d)")
    parser.add_argument("--use-cache", action="store_true", help="Allow cached LLM responses when available")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default INFO)")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    conn = psycopg2.connect(_database_url())
    store = LLMReviewStore()
    runner = LLMRunner()
    try:
        as_of = resolve_as_of(conn, args.as_of)
        entries = fetch_signal_universe(conn, as_of, args.limit)
        if not entries:
            logging.warning("No signal_universe_100 entries available for %s", as_of)
            return
        logging.info("Running weekly go/no-go for %d symbols (%s)", len(entries), as_of)
        report_items: List[Dict[str, Any]] = []
        for entry in entries:
            symbol = entry["symbol"]
            payload = _build_payload(conn, symbol=symbol, as_of=as_of, entry_meta=entry["meta"], horizon=args.horizon)
            context = {
                "symbol": symbol,
                "as_of": as_of.isoformat(),
                "forecast_horizon": args.horizon,
                "rank": entry.get("rank"),
                "signal_strength": entry.get("signal_strength"),
                "data_json": json.dumps(payload, default=str, indent=2),
            }
            try:
                resp = runner.cached_call(
                    "weekly_go_nogo",
                    context,
                    schema=WEEKLY_GO_NOGO_SCHEMA,
                    max_tokens=800,
                    use_cache=args.use_cache,
                )
            except Exception as exc:  # noqa: BLE001
                logging.warning("LLM call failed for %s: %s", symbol, exc)
                continue
            decision = resp.get("json")
            store.upsert_review(
                scope="weekly_go_nogo",
                symbol=symbol,
                as_of=as_of,
                prompt_key=resp.get("prompt_key", "weekly_go_nogo"),
                prompt_version=resp.get("prompt_version", "1.0.0"),
                prompt_hash=resp.get("prompt_hash", ""),
                output_json=decision,
                output_text=resp.get("text"),
                input_payload=payload,
                extra={
                    "rank": entry.get("rank"),
                    "signal_strength": entry.get("signal_strength"),
                    "horizon": args.horizon,
                },
                cached=bool(resp.get("cached", False)),
            )
            report_items.append(
                {
                    "symbol": symbol,
                    "decision": decision,
                    "prompt_version": resp.get("prompt_version"),
                    "context": context,
                }
            )
        if report_items:
            REPORTS_DIR.mkdir(parents=True, exist_ok=True)
            report_path = REPORTS_DIR / f"weekly_llm_go_nogo_{as_of.isoformat()}.json"
            payload = {
                "as_of": as_of.isoformat(),
                "generated_at": dt.datetime.utcnow().isoformat() + "Z",
                "horizon": args.horizon,
                "items": report_items,
            }
            report_path.write_text(json.dumps(payload, indent=2))
            logging.info("Wrote weekly go/no-go report to %s", report_path)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
