#!/usr/bin/env python3
"""Step 3: run LLM deep dives for the 100 most predictable symbols.

This script reads the latest `signal_universe_100` snapshot (produced by Step 1),
gathers the top-N rows, and reuses `run_weekly_deep_dives` to attach sentiment /
agreement metadata plus persist the results via `llm_symbol_reviews`.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
from typing import Any, Dict, List, Optional

import psycopg2

from tools.universe import build_signal_universe as universe_tools


def _database_url() -> str:
    return universe_tools.database_url()


def _resolve_as_of(conn, explicit: Optional[dt.date]) -> dt.date:
    if explicit:
        return explicit
    cur = conn.cursor()
    cur.execute("SELECT MAX(as_of) FROM signal_universe_100")
    row = cur.fetchone()
    cur.close()
    if not row or not row[0]:
        raise RuntimeError("signal_universe_100 is empty; Step 1 must run first")
    return row[0]


def _load_candidates(conn, as_of: dt.date, limit: int) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol, signal_strength, meta
        FROM signal_universe_100
        WHERE as_of=%s
        ORDER BY rank ASC
        LIMIT %s
        """,
        (as_of, limit),
    )
    rows = cur.fetchall()
    cur.close()
    entries: List[Dict[str, Any]] = []
    for symbol, signal_strength, meta in rows:
        if isinstance(meta, str):
            try:
                meta_obj = json.loads(meta)
            except Exception:
                meta_obj = {}
        else:
            meta_obj = meta or {}
        entry = {
            "symbol": symbol,
            "signal_strength": float(signal_strength or 0.0),
            "avg_dollar_vol_60d": meta_obj.get("avg_dollar_vol_60d"),
            "last_price": meta_obj.get("last_price"),
            "mean_return": meta_obj.get("mean_return"),
            "spy_mean": meta_obj.get("spy_mean"),
            "llm": meta_obj.get("llm") or {},
            "monte_carlo": meta_obj.get("monte_carlo"),
        }
        entries.append(entry)
    return entries


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Step 3: LLM weekly deep dive for top symbols.")
    parser.add_argument("--as-of", type=lambda s: dt.date.fromisoformat(s), help="Snapshot as-of date (defaults to latest)")
    parser.add_argument("--top-n", type=int, default=universe_tools.DEFAULT_LIMIT, help="Symbols to process (default 100)")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default INFO)")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    conn = psycopg2.connect(_database_url())
    try:
        as_of = _resolve_as_of(conn, args.as_of)
        entries = _load_candidates(conn, as_of, args.top_n)
        if not entries:
            logging.warning("No signal_universe_100 entries found for %s", as_of)
            return
        logging.info("Running LLM deep dive for %d symbols (%s)", len(entries), as_of)
        universe_tools.run_weekly_deep_dives(conn, entries, as_of)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
