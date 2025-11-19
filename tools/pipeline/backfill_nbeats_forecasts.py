#!/usr/bin/env python3
"""Backfill Baseline/5d forecasts for Alpaca equities using the N-BEATS artifact."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import numpy as np
import psycopg2
from psycopg2.extras import execute_batch

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in os.sys.path:
    os.sys.path.append(str(ROOT_DIR))

from libs.kronos.model_loader import load_nbeats_artifact, LocalNBeatsArtifact  # noqa: E402
from tools.universe.build_signal_universe import database_url  # noqa: E402

FORECAST_MODEL = os.getenv("PREDICTABILITY_NBEATS_PREFIX", "Baseline")
FORECAST_HORIZON_TAG = os.getenv("PREDICTABILITY_NBEATS_HORIZON", "5d")
DEFAULT_REPORTS_DIR = ROOT_DIR / "reports"


@dataclass
class SymbolSeries:
    symbol: str
    dates: List[dt.date]
    returns: List[float]


def _parse_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"Invalid date: {value}") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill N-BEATS forecasts for Alpaca equities.")
    parser.add_argument("--start-date", required=True, type=_parse_date, help="Earliest origin date (YYYY-MM-DD).")
    parser.add_argument("--end-date", required=True, type=_parse_date, help="Latest origin date (YYYY-MM-DD).")
    parser.add_argument("--horizon-days", type=int, default=5, help="Forecast horizon (default: 5).")
    parser.add_argument("--input-window", type=int, default=90, help="Required history length for inference.")
    parser.add_argument("--max-symbols", type=int, default=None, help="Optional limit on symbols processed.")
    parser.add_argument("--batch-size", type=int, default=64, help="Commit batch size (default: 64).")
    parser.add_argument(
        "--model-dir",
        type=str,
        default=os.getenv("KRONOS_NBEATS_MODEL_DIR"),
        help="Override Kronos N-BEATS artifact path.",
    )
    return parser.parse_args()


def fetch_symbols(conn, limit: Optional[int]) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT u.symbol
            FROM alpaca_universe u
            JOIN coverage_180_final c USING(symbol)
            WHERE c.last_bar IS NOT NULL
            ORDER BY u.symbol ASC
            """
        )
        rows = cur.fetchall()
    symbols = [row[0] for row in rows]
    if limit:
        symbols = symbols[:limit]
    return symbols


def load_daily_series(conn, symbol: str) -> SymbolSeries:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ds, y
            FROM daily_returns
            WHERE symbol=%s
            ORDER BY ds ASC
            """,
            (symbol,),
        )
        rows = cur.fetchall()
    dates = [row[0] for row in rows]
    returns = [float(row[1]) for row in rows]
    return SymbolSeries(symbol=symbol, dates=dates, returns=returns)


def iter_origin_dates(
    series: SymbolSeries,
    start_date: dt.date,
    end_date: dt.date,
    input_window: int,
    horizon_days: int,
) -> Iterable[tuple[dt.date, np.ndarray]]:
    dates = series.dates
    returns = series.returns
    if len(dates) < input_window:
        return
    date_to_index = {ds: idx for idx, ds in enumerate(dates)}
    current = start_date
    max_origin = end_date - dt.timedelta(days=horizon_days)
    while current <= max_origin:
        idx = date_to_index.get(current)
        if idx is not None and idx >= input_window:
            window = returns[idx - input_window : idx]
            if len(window) == input_window:
                yield current, np.array(window, dtype=np.float32)
        current += dt.timedelta(days=1)


def prepare_insert(
    origin_date: dt.date,
    symbol: str,
    horizon_days: int,
    preds: np.ndarray,
    quantiles: Sequence[float],
) -> tuple:
    arr = np.asarray(preds)
    if arr.ndim == 0:
        row = arr.reshape(1,)
    elif arr.ndim == 1:
        row = arr
    else:
        horizon_idx = min(horizon_days, arr.shape[0]) - 1
        row = arr[horizon_idx]
    n = int(row.shape[0])

    q_list = list(quantiles or [])
    q_map = {float(round(q, 4)): idx for idx, q in enumerate(q_list)}

    def safe_idx(q: float, default_idx: int) -> int:
        idx = q_map.get(q, default_idx)
        if idx < 0:
            idx = 0
        if idx >= n:
            idx = min(default_idx, n - 1)
        return idx

    if n <= 1:
        mean_val = float(row[0])
        lower_val = mean_val
        upper_val = mean_val
    else:
        mean_idx = safe_idx(0.5, default_idx=0)
        lower_idx = safe_idx(0.05, default_idx=0)
        upper_idx = safe_idx(0.95, default_idx=n - 1)
        mean_val = float(row[mean_idx])
        lower_val = float(row[lower_idx])
        upper_val = float(row[upper_idx])
    ts = dt.datetime(origin_date.year, origin_date.month, origin_date.day, tzinfo=dt.timezone.utc)
    features = {
        "nbeats_backfill": True,
        "input_window": len(row) if row.ndim == 1 else None,
        "horizon_days": horizon_days,
        "source": "backfill_nbeats_forecasts.py",
    }
    return (
        ts,
        symbol,
        FORECAST_HORIZON_TAG,
        FORECAST_MODEL,
        mean_val,
        lower_val,
        upper_val,
        json.dumps(features),
    )


def insert_forecasts(conn, rows: List[tuple]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_batch(
            cur,
            """
            INSERT INTO forecasts(ts, ticker, horizon, model, mean, lower, upper, features)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ticker, horizon, ts)
            DO NOTHING
            """,
            rows,
            page_size=256,
        )
    conn.commit()
    return len(rows)


def main() -> None:
    args = parse_args()
    if args.start_date > args.end_date:
        raise SystemExit("--start-date must be <= --end-date.")
    artifact = load_nbeats_artifact(args.model_dir)
    horizon_days = args.horizon_days
    if horizon_days > artifact.horizon:
        raise SystemExit(f"Artifact horizon {artifact.horizon} < requested {horizon_days}")

    conn = psycopg2.connect(database_url())
    conn.autocommit = False
    try:
        symbols = fetch_symbols(conn, args.max_symbols)
        print(f"[nbeats-backfill] symbols={len(symbols)} range={args.start_date}..{args.end_date}")
        total_inserted = 0
        batch_rows: List[tuple] = []
        for symbol in symbols:
            series = load_daily_series(conn, symbol)
            inserted_symbol = 0
            for origin_date, window in iter_origin_dates(
                series,
                args.start_date,
                args.end_date,
                args.input_window,
                horizon_days,
            ):
                preds = artifact.predict(window)
                row = prepare_insert(origin_date, symbol, horizon_days, preds, artifact.quantiles)
                batch_rows.append(row)
                inserted_symbol += 1
                if len(batch_rows) >= args.batch_size:
                    total_inserted += insert_forecasts(conn, batch_rows)
                    batch_rows = []
            if inserted_symbol:
                print(f"[nbeats-backfill] {symbol}: {inserted_symbol} rows")
        if batch_rows:
            total_inserted += insert_forecasts(conn, batch_rows)
        print(
            f"[nbeats-backfill] Complete: {total_inserted} forecasts inserted "
            f"({args.start_date}..{args.end_date}, horizon={horizon_days}d)"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
