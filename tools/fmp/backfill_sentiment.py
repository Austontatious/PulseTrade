"""Offline backfill utility for FMP social sentiment history."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import time
from pathlib import Path
from typing import Iterable, List, Sequence

import psycopg2
import psycopg2.extras
import requests

FMP_API_KEY = os.getenv("FMP_API_KEY")
BASE_URL = "https://financialmodelingprep.com/api/v4/historical/social-sentiment"
DEFAULT_LIMIT = int(os.getenv("FMP_SENTIMENT_LIMIT", "120"))
DEFAULT_LOOKBACK_DAYS = int(os.getenv("FMP_BACKFILL_LOOKBACK_DAYS", "420"))
MAX_ATTEMPTS = int(os.getenv("FMP_BACKFILL_MAX_ATTEMPTS", "5"))
BACKOFF_BASE = float(os.getenv("FMP_BACKFILL_BACKOFF_BASE", "2.0"))
UNIVERSE_PATH = Path("/mnt/data/PulseTrade/db/alpaca_universe.symbols.txt")

DDL = """
CREATE TABLE IF NOT EXISTS fmp_social_sentiment (
  symbol TEXT NOT NULL,
  as_of DATE NOT NULL,
  source TEXT NOT NULL,
  item_id TEXT NOT NULL,
  sentiment_score DOUBLE PRECISION,
  positive_count INTEGER,
  negative_count INTEGER,
  neutral_count INTEGER,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY(symbol, as_of, source, item_id)
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
            print(f"[backfill-sentiment] request error {exc}; retrying in {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue
        if resp.status_code in (429, 503):
            sleep_s = min(BACKOFF_BASE ** attempt, 60.0)
            print(f"[backfill-sentiment] rate limited ({resp.status_code}); retrying in {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue
        resp.raise_for_status()
        return resp
    raise RuntimeError("Max attempts exceeded for FMP sentiment request")


def _fetch(symbol: str, start: dt.date | None, end: dt.date | None, limit: int) -> Sequence[dict]:
    params = {"symbol": symbol, "apikey": FMP_API_KEY, "limit": limit}
    if start:
        params["from"] = start.isoformat()
    if end:
        params["to"] = end.isoformat()
    resp = _request_with_retry(params)
    data = resp.json()
    if not isinstance(data, list):
        return []
    return data


def _prepare_rows(symbol: str, items: Iterable[dict]) -> List[tuple]:
    rows: List[tuple] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        as_of = _parse_date(item.get("date") or item.get("publishedDate"))
        if not as_of:
            continue
        source = str(item.get("source") or "fmp").strip() or "fmp"
        item_id = str(
            item.get("articleId")
            or item.get("id")
            or item.get("url")
            or item.get("title")
            or f"{symbol}_{as_of.isoformat()}"
        )
        rows.append(
            (
                symbol,
                as_of,
                source,
                item_id,
                float(item.get("sentiment", 0.0) or 0.0),
                int(item.get("positiveMention", item.get("positiveScore", 0)) or 0),
                int(item.get("negativeMention", item.get("negativeScore", 0)) or 0),
                int(item.get("neutralMention", item.get("neutralScore", 0)) or 0),
                psycopg2.extras.Json(item),
            )
        )
    return rows


def upsert_rows(conn, rows: List[tuple]) -> None:
    if not rows:
        return
    # Deduplicate conflicting keys before sending batch
    seen: set[tuple[str, dt.date, str, str]] = set()
    deduped: List[tuple] = []
    for entry in rows:
        key = (entry[0], entry[1], entry[2], entry[3])
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
            INSERT INTO fmp_social_sentiment(
                symbol,
                as_of,
                source,
                item_id,
                sentiment_score,
                positive_count,
                negative_count,
                neutral_count,
                meta
            )
            VALUES %s
            ON CONFLICT(symbol, as_of, source, item_id)
            DO UPDATE SET
              sentiment_score = EXCLUDED.sentiment_score,
              positive_count = EXCLUDED.positive_count,
              negative_count = EXCLUDED.negative_count,
              neutral_count = EXCLUDED.neutral_count,
              updated_at = NOW(),
              meta = EXCLUDED.meta
            """,
            deduped,
            page_size=200,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill FMP historical social sentiment.")
    parser.add_argument("--symbols-file", type=Path, default=None, help="Optional file with symbols (default Alpaca universe).")
    parser.add_argument("--since", type=lambda s: dt.date.fromisoformat(s), default=None, help="Start date (YYYY-MM-DD).")
    parser.add_argument("--until", type=lambda s: dt.date.fromisoformat(s), default=None, help="End date (YYYY-MM-DD).")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max records per request.")
    return parser.parse_args()


def _load_latest_sentiment(conn) -> dict[str, dt.date]:
    with conn.cursor() as cur:
        cur.execute("SELECT symbol, MAX(as_of) FROM fmp_social_sentiment GROUP BY symbol")
        rows = cur.fetchall()
    mapping: dict[str, dt.date] = {}
    for symbol, max_date in rows:
        if isinstance(symbol, str) and isinstance(max_date, dt.date):
            mapping[symbol.upper()] = max_date
    return mapping


def main() -> None:
    if not FMP_API_KEY:
        raise SystemExit("FMP_API_KEY missing – cannot backfill sentiment.")
    args = parse_args()
    base_since = args.since or (dt.date.today() - dt.timedelta(days=DEFAULT_LOOKBACK_DAYS))
    symbols = load_symbols(args.symbols_file)
    conn = psycopg2.connect(database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
        latest_map = _load_latest_sentiment(conn)
        total_rows = 0
        for sym in symbols:
            start = base_since
            latest = latest_map.get(sym)
            if latest:
                next_day = latest + dt.timedelta(days=1)
                start = max(start, next_day)
            if args.until and start and start > args.until:
                continue
            try:
                payload = _fetch(sym, start, args.until, args.limit)
            except Exception as exc:
                print(f"[backfill-sentiment] {sym} failed: {exc}")
                continue
            rows = _prepare_rows(sym, payload)
            if not rows:
                continue
            upsert_rows(conn, rows)
            conn.commit()
            total_rows += len(rows)
            print(f"[backfill-sentiment] {sym}: {len(rows)} rows")
        print(f"[backfill-sentiment] Completed; total rows upserted: {total_rows}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
