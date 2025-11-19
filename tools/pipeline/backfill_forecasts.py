#!/usr/bin/env python3
"""Backfill Kronos forecasts for historical (symbol, as_of) pairs."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sys
from pathlib import Path
from typing import Dict, List, Sequence

import numpy as np
import psycopg2

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from libs.kronos.model_loader import load_nbeats_artifact  # noqa: E402
from libs.universe.forecast_utils import load_return_window  # noqa: E402
from tools.universe.build_signal_universe import database_url  # noqa: E402

DEFAULT_HORIZON_DAYS = 5
DEFAULT_MAX_SYMBOLS = 500
DEFAULT_MAX_DATES = 40
MODEL_NAME = "kronos-nbeats"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Kronos forecasts for historical dates.")
    parser.add_argument("--start", required=True, type=str, help="Earliest as_of date (YYYY-MM-DD).")
    parser.add_argument("--end", required=True, type=str, help="Latest as_of date (YYYY-MM-DD).")
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON_DAYS, help="Horizon in days (default: 5).")
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=DEFAULT_MAX_SYMBOLS,
        help="Max number of symbols to include (default: 500).",
    )
    parser.add_argument(
        "--max-dates",
        type=int,
        default=DEFAULT_MAX_DATES,
        help="Max number of as_of dates to evaluate (default: 40).",
    )
    parser.add_argument(
        "--model-dir",
        type=str,
        default=os.getenv("KRONOS_NBEATS_MODEL_DIR"),
        help="Optional override for the Kronos N-BEATS artifact directory.",
    )
    return parser.parse_args()


def _to_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def fetch_symbols(conn, end_date: dt.date, limit: int) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT u.symbol
            FROM alpaca_universe u
            JOIN coverage_180_final cov USING (symbol)
            WHERE cov.last_bar >= %s - INTERVAL '3 days'
            ORDER BY u.symbol ASC
            LIMIT %s
            """,
            (end_date, limit),
        )
        return [row[0] for row in cur.fetchall()]


def fetch_as_of_dates(conn, start: dt.date, end: dt.date, max_dates: int) -> List[dt.date]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ds
            FROM daily_returns
            WHERE ds BETWEEN %s AND %s
            ORDER BY ds DESC
            """,
            (start, end),
        )
        rows = [row[0] for row in cur.fetchall()]
    if len(rows) > max_dates:
        rows = rows[:max_dates]
    return sorted(rows)


def forecast_exists(conn, symbol: str, horizon_label: str, ts: dt.datetime) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM forecasts
            WHERE ticker=%s
              AND horizon=%s
              AND ts=%s
            LIMIT 1
            """,
            (symbol, horizon_label, ts),
        )
        return cur.fetchone() is not None


def _series_to_features(
    quantiles: Sequence[float],
    preds: np.ndarray,
    model_version: str,
) -> Dict[str, Dict[str, object]]:
    q_map = {}
    yhat = {}
    for step_idx in range(preds.shape[0]):
        step_key = str(step_idx + 1)
        row = preds[step_idx]
        if row.ndim == 0:
            value = float(row)
            yhat[step_key] = value
            q_map[step_key] = {"p50": value}
            continue
        entry = {}
        for q_idx, q in enumerate(quantiles):
                entry[f"p{int(round(q * 100)):02d}"] = float(row[q_idx])
                if abs(q - 0.5) < 1e-6:
                    yhat[step_key] = float(row[q_idx])
        q_map[step_key] = entry
        if step_key not in yhat and quantiles:
            yhat[step_key] = float(row[min(len(row) - 1, math.ceil(len(row) / 2))])
    features = {
        "kronos": {
            "yhat": yhat,
            "q": q_map,
            "meta": {"model": model_version, "source": "forecast_backfill"},
        }
    }
    return features


def insert_forecast(
    conn,
    ts: dt.datetime,
    symbol: str,
    horizon_label: str,
    mean_val: float,
    lower_val: float,
    upper_val: float,
    features: Dict[str, object],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO forecasts(ts, ticker, horizon, model, mean, lower, upper, features)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (ts, symbol, horizon_label, MODEL_NAME, mean_val, lower_val, upper_val, json.dumps(features)),
        )


def main() -> None:
    args = parse_args()
    start_date = _to_date(args.start)
    end_date = _to_date(args.end)
    if start_date > end_date:
        raise SystemExit("--start must be <= --end")
    conn = psycopg2.connect(database_url())
    try:
        symbols = fetch_symbols(conn, end_date, args.max_symbols)
        if not symbols:
            print("No symbols matched coverage criteria; exiting.")
            return
        as_of_dates = fetch_as_of_dates(conn, start_date, end_date, args.max_dates)
        if not as_of_dates:
            print("No trading dates in requested range; exiting.")
            return
        artifact = load_nbeats_artifact(args.model_dir)
        horizon_label = f"{args.horizon}d"
        horizon_idx = min(args.horizon, artifact.horizon) - 1
        quantiles = list(artifact.quantiles)
        q_idx = {round(q, 4): idx for idx, q in enumerate(quantiles)}
        q05_idx = q_idx.get(0.05, q_idx.get(0.0500))
        q50_idx = q_idx.get(0.5, q_idx.get(0.50))
        q95_idx = q_idx.get(0.95, q_idx.get(0.9500))
        inserted = 0
        for as_of in as_of_dates:
            ts = dt.datetime.combine(as_of, dt.time.min, tzinfo=dt.timezone.utc)
            for symbol in symbols:
                if forecast_exists(conn, symbol, horizon_label, ts):
                    continue
                series = load_return_window(conn, symbol, as_of, artifact.input_size)
                if series is None:
                    continue
                preds = artifact.predict(series)
                if preds.shape[0] <= horizon_idx:
                    continue
                row = preds[horizon_idx]
                mean_val = float(row[q50_idx]) if q50_idx is not None else float(np.median(row))
                lower_val = float(row[q05_idx]) if q05_idx is not None else float(np.min(row))
                upper_val = float(row[q95_idx]) if q95_idx is not None else float(np.max(row))
                features = _series_to_features(quantiles, preds, artifact.version)
                features.setdefault("backfill", {})["as_of"] = as_of.isoformat()
                insert_forecast(conn, ts, symbol, horizon_label, mean_val, lower_val, upper_val, features)
                inserted += 1
                if inserted % 500 == 0:
                    conn.commit()
                    print(f"Inserted {inserted} forecasts so far...")
        conn.commit()
        print(f"Backfill complete: inserted {inserted} forecast rows.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
