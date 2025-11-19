import argparse
import datetime as dt
import os
import sys
import time
from typing import Dict, Iterable, List, Optional

import psycopg2
import psycopg2.extras as extras
import requests


FMP_API_KEY = os.getenv("FMP_API_KEY")
BASE_URL = "https://financialmodelingprep.com/api/v3"
RATE = float(os.getenv("RATE_MAX_HTTP_PER_SEC", "5"))
SYMBOL_SLEEP = os.getenv("FMP_SYMBOL_SLEEP_SECS")
if SYMBOL_SLEEP is not None:
    try:
        SLEEP = max(float(SYMBOL_SLEEP), 0.0)
    except ValueError:
        SLEEP = 1.0 / max(RATE, 1.0)
else:
    SLEEP = 1.0 / max(RATE, 1.0)

ENDPOINTS: Dict[str, str] = {
    "fmp_income_statement": "income-statement/{sym}",
    "fmp_balance_sheet": "balance-sheet-statement/{sym}",
    "fmp_cash_flow": "cash-flow-statement/{sym}",
    "fmp_key_metrics": "key-metrics/{sym}",
    "fmp_ratios": "ratios/{sym}",
}


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


def read_universe(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as handle:
        symbols = [
            line.strip().upper()
            for line in handle
            if line.strip() and not line.lstrip().startswith("#")
        ]
    deduped: Dict[str, None] = {}
    for sym in symbols:
        deduped.setdefault(sym, None)
    return list(deduped.keys())


def fetch(session: requests.Session, path: str, period: str, limit: int) -> List[dict]:
    params = {"apikey": FMP_API_KEY, "period": period, "limit": limit}
    url = f"{BASE_URL}/{path}"
    for attempt in range(3):
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                return data
            return []
        time.sleep(2 ** attempt)
    resp.raise_for_status()
    return []


def to_rows(symbol: str, period: str, payload: Iterable[dict], since: dt.date) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for item in payload or []:
        raw_date: Optional[str] = item.get("date") or item.get("filingDate")
        if not raw_date:
            continue
        try:
            parsed = dt.date.fromisoformat(raw_date[:10])
        except ValueError:
            continue
        if parsed < since:
            continue
        rows.append(
            {
                "symbol": symbol,
                "period": period,
                "date": parsed,
                "payload": extras.Json(item),
            }
        )
    return rows


def upsert(conn, table: str, rows: List[Dict[str, object]]) -> None:
    if not rows:
        return
    cols = ["symbol", "period", "date", "payload"]
    # de-duplicate within batch to avoid ON CONFLICT multiple hits
    seen = set()
    dedup_values: List[List[object]] = []
    for row in rows:
        key = (row["symbol"], row["period"], row["date"])
        if key in seen:
            continue
        seen.add(key)
        dedup_values.append([row[c] for c in cols])
    query = f"""
        INSERT INTO {table} ({', '.join(cols)})
        VALUES %s
        ON CONFLICT (symbol, period, date) DO UPDATE
        SET payload = EXCLUDED.payload,
            ts = now();
    """
    with conn.cursor() as cur:
        extras.execute_values(cur, query, dedup_values, page_size=200)
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill FMP fundamentals payload tables.")
    parser.add_argument("--period", default="annual", choices=["annual", "quarter"])
    parser.add_argument("--since", default="2005-01-01")
    parser.add_argument("--universe", required=True, help="Path to newline-delimited symbols.")
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args()

    if not FMP_API_KEY:
        sys.exit("FMP_API_KEY not set; aborting.")

    try:
        since = dt.date.fromisoformat(args.since)
    except ValueError as exc:
        raise SystemExit(f"Invalid --since date: {args.since}") from exc

    symbols = read_universe(args.universe)
    if not symbols:
        sys.exit("Universe file is empty.")

    conn = psycopg2.connect(database_url())
    conn.autocommit = False
    session = requests.Session()

    for idx, symbol in enumerate(symbols, start=1):
        print(f"[{idx}/{len(symbols)}] {symbol}")
        for table, endpoint in ENDPOINTS.items():
            try:
                payload = fetch(session, endpoint.format(sym=symbol), args.period, args.limit)
                rows = to_rows(symbol, args.period, payload, since)
                upsert(conn, table, rows)
            except Exception as exc:  # pragma: no cover - defensive logging
                print(f"!! {symbol} {table}: {exc}")
        time.sleep(SLEEP)

    conn.close()
    session.close()


if __name__ == "__main__":
    main()
