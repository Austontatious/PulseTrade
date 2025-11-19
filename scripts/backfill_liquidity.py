#!/usr/bin/env python3
"""Backfill symbol last price and 60d liquidity using the Alpaca data API.

This script:
  * Finds equities that are missing `meta.last_price` or do not have liquidity coverage.
  * Pulls the most recent daily bars from Alpaca.
  * Loads the derived returns + dollar volume into `daily_returns`.
  * Updates the `symbols.meta` payload with `last_price` and `avg_dollar_vol_60d`.
  * Refreshes `mv_liquidity_60d` so downstream screens (`build_signal_universe.py`) see the data.

Run inside the tools container:

    docker compose run --rm tools \
      bash -lc 'python scripts/backfill_liquidity.py --limit 250'
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import math
import os
import sys
import time
from dataclasses import dataclass
import re
from pathlib import Path
from typing import List, Sequence, Tuple

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)


def _load_env_from_file() -> None:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / ".env"
        if candidate.exists():
            load_dotenv(dotenv_path=candidate, override=False)
            break


_load_env_from_file()

DEFAULT_LOOKBACK = int(os.getenv("LIQUIDITY_BACKFILL_LOOKBACK_DAYS", "90"))
DEFAULT_LIMIT = int(os.getenv("LIQUIDITY_BACKFILL_LIMIT", "300"))
DEFAULT_MIN_LIQ = float(os.getenv("LIQUIDITY_BACKFILL_MIN_DOLLAR_VOL", "1000000"))
DEFAULT_SLEEP = float(os.getenv("LIQUIDITY_BACKFILL_THROTTLE_SECS", "0.2"))
MAX_BACKOFF_S = float(os.getenv("LIQUIDITY_BACKFILL_MAX_BACKOFF_S", "30.0"))

ALPACA_DATA_BASE = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
ALPACA_DATA_FEED = os.getenv("ALPACA_DATA_FEED", "iex")


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


def _psycopg_connection() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(database_url())
    conn.autocommit = False
    return conn


def _alpaca_session() -> requests.Session:
    key = os.getenv("ALPACA_API_KEY_ID")
    secret = os.getenv("ALPACA_API_SECRET_KEY")
    if not key or not secret:
        raise SystemExit("ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY must be set for backfill.")
    session = requests.Session()
    session.headers.update(
        {
            "APCA-API-KEY-ID": key,
            "APCA-API-SECRET-KEY": secret,
            "Accept": "application/json",
        }
    )
    return session


VALID_SYMBOL_RE = re.compile(r"^[A-Z0-9_.]+$")


def sanitise_api_symbol(symbol: str) -> str | None:
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    sym = sym.replace("-", "_").replace(" ", "")
    if not VALID_SYMBOL_RE.match(sym):
        return None
    return sym


def _parse_bar_ts(value: str) -> dt.date:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return dt.datetime.fromisoformat(value).date()


def fetch_daily_bars(
    session: requests.Session,
    api_symbol: str,
    lookback: int,
    base_sleep: float,
) -> list[dict]:
    url = f"{ALPACA_DATA_BASE}/v2/stocks/{api_symbol}/bars"
    params = {
        "timeframe": "1Day",
        "limit": lookback,
        "adjustment": "split",
        "feed": ALPACA_DATA_FEED,
    }
    backoff = max(base_sleep, 0.5)
    while True:
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 404:
            logging.warning("No bars returned for %s (404)", api_symbol)
            return []
        if resp.status_code == 400:
            logging.warning("Skipping %s (400 Bad Request)", api_symbol)
            return []
        if resp.status_code == 429:
            logging.warning("Rate limited for %s; sleeping %.1fs", api_symbol, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_S)
            continue
        resp.raise_for_status()
        payload = resp.json()
        bars = payload.get("bars") or []
        return sorted(bars, key=lambda item: item.get("t", ""))


@dataclass
class SymbolPayload:
    ticker: str
    last_price: float
    avg_dollar_vol: float
    rows: List[Tuple[dt.date, str, float, float]]


def build_payload(symbol: str, bars: Sequence[dict]) -> SymbolPayload | None:
    if not bars:
        return None
    prev_close: float | None = None
    rows: List[Tuple[dt.date, str, float, float]] = []
    dollar_vol_accumulator: List[float] = []
    last_price = 0.0

    for bar in bars:
        close = float(bar.get("c") or 0.0)
        volume = float(bar.get("v") or 0.0)
        trade_date = _parse_bar_ts(bar["t"])
        dollar_volume = close * volume
        dollar_vol_accumulator.append(dollar_volume)
        if prev_close and prev_close > 0 and close > 0:
            log_return = math.log(close / prev_close)
            rows.append((trade_date, symbol, log_return, dollar_volume))
        prev_close = close
        last_price = close

    if not rows:
        logging.warning("Insufficient bar history to build returns for %s", symbol)
        return None

    avg_dollar_vol = sum(dollar_vol_accumulator) / max(len(dollar_vol_accumulator), 1)
    return SymbolPayload(symbol, last_price, avg_dollar_vol, rows)


def upsert_daily_returns(conn, payload: SymbolPayload) -> int:
    if not payload.rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO daily_returns (ds, symbol, y, dollar_vol)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol, ds)
            DO UPDATE SET
                y = EXCLUDED.y,
                dollar_vol = EXCLUDED.dollar_vol
            """,
            payload.rows,
            page_size=100,
        )
    return len(payload.rows)


def update_symbol_meta(conn, payload: SymbolPayload) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE symbols
            SET meta = jsonb_set(
                jsonb_set(
                    COALESCE(meta, '{}'::jsonb),
                    '{last_price}',
                    to_jsonb(%s::numeric),
                    true
                ),
                '{avg_dollar_vol_60d}',
                to_jsonb(%s::numeric),
                true
            )
            WHERE ticker = %s AND class = 'equity'
            """,
            (payload.last_price, payload.avg_dollar_vol, payload.ticker),
        )


def fetch_candidate_symbols(
    conn,
    limit: int,
    min_liquidity: float,
    explicit: Sequence[str] | None = None,
) -> list[str]:
    symbols: dict[str, None] = {}
    if explicit:
        for s in explicit:
            symbols[s.upper()] = None

    remaining = max(limit - len(symbols), 0)
    if remaining > 0:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.ticker
                FROM symbols s
                LEFT JOIN mv_liquidity_60d l ON l.symbol = s.ticker
                WHERE s.class = 'equity'
                  AND (
                        COALESCE(NULLIF(s.meta->>'last_price','')::numeric, 0) <= 0
                     OR COALESCE(l.avg_dollar_vol_60d, 0) < %s
                  )
                ORDER BY s.ticker
                LIMIT %s
                """,
                (min_liquidity, remaining),
            )
            for row in cur.fetchall():
                symbols[row[0]] = None
    return sorted(symbols.keys())


def refresh_materialized_view(conn) -> None:
    with conn.cursor() as cur:
        logging.info("Refreshing mv_liquidity_60d ...")
        cur.execute("REFRESH MATERIALIZED VIEW mv_liquidity_60d")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill liquidity + last price data via Alpaca")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max symbols to backfill")
    parser.add_argument(
        "--min-liquidity",
        type=float,
        default=DEFAULT_MIN_LIQ,
        help="Treat names below this dollar volume as stale/missing",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK,
        help="Number of daily bars to request for each symbol",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP,
        help="Seconds to sleep between Alpaca calls",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Optional explicit tickers (comma or space separated)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Fetch data without writing to the DB")
    parser.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Skip refreshing mv_liquidity_60d after inserts",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = parse_args()
    session = _alpaca_session()
    conn = _psycopg_connection()
    try:
        explicit = []
        if args.symbols:
            for token in args.symbols:
                explicit.extend(sym.strip().upper() for sym in token.split(",") if sym.strip())
        candidates = fetch_candidate_symbols(conn, args.limit, args.min_liquidity, explicit)
        if not candidates:
            logging.info("No symbols require backfill. Exiting.")
            return

        logging.info("Backfilling %d symbols (lookback=%d days)", len(candidates), args.lookback_days)
        total_rows = 0
        processed = 0

        for symbol in candidates:
            api_symbol = sanitise_api_symbol(symbol)
            if not api_symbol:
                logging.warning("Skipping invalid symbol format: %s", symbol)
                continue
            try:
                bars = fetch_daily_bars(session, api_symbol, args.lookback_days, args.sleep)
            except requests.HTTPError as exc:
                logging.error("HTTP error for %s: %s", symbol, exc)
                continue
            except Exception as exc:  # noqa: BLE001
                logging.exception("Unexpected error fetching %s", symbol)
                continue

            payload = build_payload(symbol, bars)
            if not payload:
                continue

            if args.dry_run:
                logging.info(
                    "[dry-run] Would update %s (last_price=%.2f, liquidity=%.0f, rows=%d)",
                    symbol,
                    payload.last_price,
                    payload.avg_dollar_vol,
                    len(payload.rows),
                )
            else:
                rows_written = upsert_daily_returns(conn, payload)
                update_symbol_meta(conn, payload)
                conn.commit()
                total_rows += rows_written
                logging.info(
                    "Updated %s: last_price=%.2f avg_dollar_vol=%.0f (%d rows)",
                    symbol,
                    payload.last_price,
                    payload.avg_dollar_vol,
                    rows_written,
                )

            processed += 1
            if args.sleep > 0:
                time.sleep(args.sleep)

        if not args.dry_run and not args.skip_refresh:
            refresh_materialized_view(conn)
            conn.commit()

        logging.info("Backfill complete: %d symbols, %d daily_return rows", processed, total_rows)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
