"""Backfill FMP v4 price target consensus data into PostgreSQL."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import time
from pathlib import Path
from typing import Iterable, List

import psycopg2
import psycopg2.extras
import requests

FMP_API_KEY = os.getenv("FMP_API_KEY")
BASE_URL = "https://financialmodelingprep.com/api/v4/price-target"
UNIVERSE_PATH = Path("/mnt/data/PulseTrade/db/alpaca_universe.symbols.txt")
DEFAULT_LOOKBACK_DAYS = int(os.getenv("FMP_BACKFILL_LOOKBACK_DAYS", "420"))
MAX_ATTEMPTS = int(os.getenv("FMP_BACKFILL_MAX_ATTEMPTS", "5"))
BACKOFF_BASE = float(os.getenv("FMP_BACKFILL_BACKOFF_BASE", "2.0"))

DDL = """
CREATE TABLE IF NOT EXISTS fmp_price_targets_v4 (
  symbol TEXT NOT NULL,
  published_date DATE NOT NULL,
  target_high DOUBLE PRECISION,
  target_low DOUBLE PRECISION,
  target_avg DOUBLE PRECISION,
  target_current DOUBLE PRECISION,
  analyst_count INTEGER,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY(symbol, published_date)
);
"""


def database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    user = os.getenv("POSTGRES_USER", "pulse")
    password = os.getenv("POSTGRES_PASSWORD", "pulsepass")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    name = os.getenv("POSTGRES_DB", "pulse")
    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


def load_symbols(path: Path | None) -> List[str]:
    source = path or UNIVERSE_PATH
    if not source.exists():
        raise SystemExit(f"Symbol file not found: {source}")
    with source.open("r", encoding="utf-8") as handle:
        symbols = [line.strip().upper() for line in handle if line.strip() and not line.startswith("#")]
    return sorted(set(symbols))


def _parse_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(value[:10])
    except Exception:
        return None


def _request_with_retry(params: dict) -> requests.Response:
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        attempt += 1
        try:
            resp = requests.get(BASE_URL, params=params, timeout=20)
        except requests.RequestException as exc:
            if attempt >= MAX_ATTEMPTS:
                raise
            sleep_s = min(BACKOFF_BASE ** (attempt - 1), 30.0)
            print(f"[backfill-price-targets] request error {exc}; retrying in {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue
        if resp.status_code in (429, 503):
            sleep_s = min(BACKOFF_BASE ** attempt, 60.0)
            print(f"[backfill-price-targets] rate limited ({resp.status_code}); retrying in {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue
        resp.raise_for_status()
        return resp
    raise RuntimeError("Max attempts exceeded for FMP price target request")


def _fetch(symbol: str, limit: int, start: dt.date | None, end: dt.date | None) -> Iterable[dict]:
    params = {"symbol": symbol, "limit": limit, "apikey": FMP_API_KEY}
    if start:
        params["from"] = start.isoformat()
    if end:
        params["to"] = end.isoformat()
    resp = _request_with_retry(params)
    payload = resp.json()
    if isinstance(payload, list):
        return payload
    return []


def _prepare_rows(symbol: str, items: Iterable[dict]) -> List[tuple]:
    rows: List[tuple] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        published = _parse_date(item.get("publishedDate") or item.get("createdAt"))
        if not published:
            continue
        rows.append(
            (
                symbol,
                published,
                float(item.get("priceTargetHigh") or item.get("targetHigh") or 0.0),
                float(item.get("priceTargetLow") or item.get("targetLow") or 0.0),
                float(item.get("priceTargetAverage") or item.get("targetMean") or 0.0),
                float(item.get("priceTargetCurrent") or item.get("currentPrice") or 0.0),
                int(item.get("numberOfAnalysts") or item.get("analystRatings") or 0),
                psycopg2.extras.Json(item),
            )
        )
    return rows


def upsert(conn, rows: List[tuple]) -> None:
    if not rows:
        return
    seen: set[tuple[str, dt.date]] = set()
    deduped: List[tuple] = []
    for entry in rows:
        key = (entry[0], entry[1])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
    if not deduped:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO fmp_price_targets_v4(
                symbol,
                published_date,
                target_high,
                target_low,
                target_avg,
                target_current,
                analyst_count,
                meta
            )
            VALUES %s
            ON CONFLICT(symbol, published_date)
            DO UPDATE SET
              target_high = EXCLUDED.target_high,
              target_low = EXCLUDED.target_low,
              target_avg = EXCLUDED.target_avg,
              target_current = EXCLUDED.target_current,
              analyst_count = EXCLUDED.analyst_count,
              meta = EXCLUDED.meta,
              updated_at = NOW()
            """,
            deduped,
            page_size=200,
        )


def _load_latest_targets(conn) -> dict[str, dt.date]:
    with conn.cursor() as cur:
        cur.execute("SELECT symbol, MAX(published_date) FROM fmp_price_targets_v4 GROUP BY symbol")
        rows = cur.fetchall()
    mapping: dict[str, dt.date] = {}
    for symbol, max_date in rows:
        if isinstance(symbol, str) and isinstance(max_date, dt.date):
            mapping[symbol.upper()] = max_date
    return mapping


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill FMP v4 price targets.")
    parser.add_argument("--symbols-file", type=Path, default=None, help="Optional file with Alpaca universe symbols.")
    parser.add_argument("--limit", type=int, default=int(os.getenv("FMP_PRICE_TARGET_LIMIT", "200")), help="Max API rows.")
    parser.add_argument("--since", type=lambda s: dt.date.fromisoformat(s), default=None, help="Start date (YYYY-MM-DD).")
    parser.add_argument("--until", type=lambda s: dt.date.fromisoformat(s), default=None, help="End date (YYYY-MM-DD).")
    return parser.parse_args()


def main() -> None:
    if not FMP_API_KEY:
        raise SystemExit("FMP_API_KEY missing – cannot backfill price targets.")
    args = parse_args()
    symbols = load_symbols(args.symbols_file)
    base_since = args.since or (dt.date.today() - dt.timedelta(days=DEFAULT_LOOKBACK_DAYS))
    conn = psycopg2.connect(database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
        latest_map = _load_latest_targets(conn)
        total = 0
        for sym in symbols:
            start = base_since
            latest = latest_map.get(sym)
            if latest:
                next_day = latest + dt.timedelta(days=1)
                start = max(start, next_day)
            if args.until and start > args.until:
                continue
            try:
                payload = _fetch(sym, args.limit, start, args.until)
            except Exception as exc:
                print(f"[backfill-price-targets] {sym} failed: {exc}")
                continue
            rows = _prepare_rows(sym, payload)
            if not rows:
                continue
            upsert(conn, rows)
            conn.commit()
            total += len(rows)
            print(f"[backfill-price-targets] {sym}: {len(rows)} rows")
        print(f"[backfill-price-targets] Completed; total rows upserted: {total}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
