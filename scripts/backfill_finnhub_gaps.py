#!/usr/bin/env python3
from __future__ import annotations
import argparse, os, time, math, datetime as dt
from pathlib import Path
from typing import List, Tuple

import psycopg2
import psycopg2.extras as extras
import requests
from dotenv import load_dotenv

# --- project root + .env ---
ROOT = Path(__file__).resolve().parents[1]  # repo root
ENV = next((p/".env" for p in [ROOT, *ROOT.parents] if (p/".env").exists()), None)
if ENV:
    load_dotenv(dotenv_path=ENV, override=False)

# --- defaults (can be overridden by CLI or env) ---
DEF_LOOKBACK = int(os.getenv("GAP_LOOKBACK_DAYS", "120"))
DEF_MIN_NEED = int(os.getenv("GAP_MIN_NEED_DAYS", "80"))
DEF_LIMIT    = int(os.getenv("GAP_PATCH_LIMIT", "400"))
DEF_SLEEP    = float(os.getenv("GAP_SYMBOL_SLEEP", "0.25"))
DEF_FILE     = os.getenv("ALPACA_UNIVERSE_FILE", str(ROOT / "db" / "alpaca_universe.symbols.txt"))

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

def db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url: return url
    user = os.getenv("POSTGRES_USER", "pulse")
    pwd  = os.getenv("POSTGRES_PASSWORD", "pulsepass")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    name = os.getenv("POSTGRES_DB", "pulse")
    return f"postgresql://{user}:{pwd}@{host}:{port}/{name}"

RELATIVE_GAPS_SQL = """
WITH params AS (
  SELECT CURRENT_DATE::date AS today,
         (CURRENT_DATE - %s::int)::date AS window_start,
         %s::int AS slack
),
base AS (
  SELECT symbol FROM {scope}
),
have AS (
  SELECT dr.symbol, COUNT(*) AS have_days
  FROM daily_returns dr
  JOIN base b ON b.symbol = dr.symbol, params p
  WHERE dr.ds >= p.window_start
  GROUP BY dr.symbol
),
per AS (
  SELECT
    b.symbol,
    COALESCE(h.have_days, 0) AS have_days,
    /* earliest day we actually have (overall), capped to window_start */
    GREATEST(
      COALESCE((
        SELECT MIN(dr2.ds)::date
        FROM daily_returns dr2
        WHERE dr2.symbol = b.symbol
      ), (SELECT window_start FROM params)),
      (SELECT window_start FROM params)
    ) AS eff_start
  FROM base b
  LEFT JOIN have h USING(symbol)
),
need AS (
  SELECT
    p.symbol,
    p.have_days,
    (
      SELECT COUNT(*)
      FROM generate_series(p.eff_start, (SELECT today FROM params), INTERVAL '1 day') g(d)
      WHERE EXTRACT(ISODOW FROM g.d) BETWEEN 1 AND 5
    ) AS expected_days
  FROM per p
)
SELECT symbol
FROM need
WHERE have_days < GREATEST(expected_days - 1 - (SELECT slack FROM params), 0)
   OR have_days < %s
ORDER BY (expected_days - have_days) DESC
LIMIT %s;
"""


def _load_temp_universe(conn, symbols_file: str) -> str:
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tmp_universe(symbol text PRIMARY KEY) ON COMMIT DROP;")
        rows = []
        with open(symbols_file, "r", encoding="utf-8") as fh:
            for ln in fh:
                s = ln.strip().upper()
                if s and not s.startswith("#"):
                    rows.append((s,))
        if rows:
            extras.execute_batch(cur, "INSERT INTO tmp_universe(symbol) VALUES (%s) ON CONFLICT DO NOTHING;", rows, page_size=1000)
    return "tmp_universe"

def select_needy_symbols(conn, scope_table: str, window_days: int, slack: int, min_need: int, limit: int) -> List[str]:
    sql = RELATIVE_GAPS_SQL.format(scope=scope_table)
    with conn.cursor() as cur:
        cur.execute(sql, (window_days, slack, min_need, limit))
        return [row[0] for row in cur.fetchall()]

def finnhub_candles(symbol: str, start_ts: int, end_ts: int) -> Tuple[List[Tuple[dt.date, str, float, float]], float, float] | None:
    url = "https://finnhub.io/api/v1/stock/candle"
    params = {"symbol": symbol, "resolution": "D", "from": start_ts, "to": end_ts, "token": FINNHUB_API_KEY}
    backoff = 1.0
    for _ in range(4):
        r = requests.get(url, params=params, timeout=20)
        if r.status_code == 429:
            time.sleep(backoff); backoff = min(backoff * 2, 16.0); continue
        r.raise_for_status()
        j = r.json() or {}
        if j.get("s") != "ok": return None
        c, v, t = j.get("c") or [], j.get("v") or [], j.get("t") or []
        if not c or not v or not t: return None
        rows: List[Tuple[dt.date, str, float, float]] = []
        prev = None; dv_acc = []
        for ts, close, vol in zip(t, c, v):
            if close is None or vol is None: continue
            close = float(close); vol = float(vol)
            d = dt.datetime.utcfromtimestamp(int(ts)).date()
            dv = close * vol; dv_acc.append(dv)
            if prev and prev > 0 and close > 0:
                y = math.log(close / prev)
                rows.append((d, symbol, y, dv))
            prev = close
        if not rows: return None
        last_price = float(c[-1])
        avg_dollar_vol = sum(dv_acc) / max(1, len(dv_acc))
        return rows, last_price, avg_dollar_vol
    return None

def alpaca_bars(symbol: str, start_iso: str, end_iso: str) -> List[Tuple[dt.date, str, float, float]]:
    base = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
    feed = os.getenv("ALPACA_DATA_FEED", "iex")
    url = f"{base}/v2/stocks/bars"
    params = {
        "timeframe": "1Day",
        "symbols": symbol,
        "start": start_iso,
        "end": end_iso,
        "adjustment": "split",
        "feed": feed,
        "limit": 10000,
    }
    headers = {
        "APCA-API-KEY-ID": os.getenv("ALPACA_API_KEY_ID", ""),
        "APCA-API-SECRET-KEY": os.getenv("ALPACA_API_SECRET_KEY", ""),
        "Accept": "application/json",
    }
    r = requests.get(url, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json() or {}
    bars = data.get("bars") or {}
    if isinstance(bars, list):
        entries = bars
    else:
        entries = bars.get(symbol) or []
    rows: List[Tuple[dt.date, str, float, float]] = []
    prev = None
    for b in entries:
        if not isinstance(b, dict):
            continue
        close = float(b.get("c") or 0.0)
        vol = float(b.get("v") or 0.0)
        tval = (b.get("t") or "").replace("Z", "+00:00")
        if close <= 0 or vol < 0:
            continue
        try:
            day = dt.datetime.fromisoformat(tval).date()
        except Exception:
            continue
        if prev and prev > 0 and close > 0:
            rows.append((day, symbol, math.log(close / prev), close * vol))
        prev = close
    return rows

def upsert_returns(conn, rows: List[Tuple[dt.date, str, float, float]]) -> int:
    if not rows: return 0
    with conn.cursor() as cur:
        extras.execute_batch(cur, """
          INSERT INTO daily_returns (ds, symbol, y, dollar_vol)
          VALUES (%s, %s, %s, %s)
          ON CONFLICT (symbol, ds)
          DO UPDATE SET y = EXCLUDED.y, dollar_vol = EXCLUDED.dollar_vol
        """, rows, page_size=500)
    conn.commit(); return len(rows)

def update_symbol_meta(conn, symbol: str, last_price: float, avg_dollar_vol: float) -> None:
    with conn.cursor() as cur:
        cur.execute("""
          UPDATE symbols
          SET meta = jsonb_set(
                      jsonb_set(COALESCE(meta,'{}'::jsonb),
                                '{last_price}', to_jsonb(%s::numeric), true),
                      '{avg_dollar_vol_60d}', to_jsonb(%s::numeric), true)
          WHERE ticker=%s AND class='equity'
        """, (last_price, avg_dollar_vol, symbol))
    conn.commit()

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Patch missing daily_returns rows via Finnhub for the Alpaca universe.")
    ap.add_argument("--lookback", type=int, default=DEF_LOOKBACK, help="Calendar days to look back (default 120)")
    ap.add_argument("--min-need", type=int, default=DEF_MIN_NEED, help="Patch names with < this many rows (default 80)")
    ap.add_argument("--limit", type=int, default=DEF_LIMIT, help="Max symbols to patch this run (default 400)")
    ap.add_argument("--sleep", type=float, default=DEF_SLEEP, help="Sleep seconds between symbols (default 0.25)")
    ap.add_argument("--symbols-file", default=DEF_FILE, help="Path to universe symbols file (default db/alpaca_universe.symbols.txt)")
    ap.add_argument("--use-table", action="store_true", help="Scope from DB table alpaca_universe instead of symbols file")
    ap.add_argument("--provider", choices=["auto", "finnhub", "alpaca"], default="auto", help="Data source preference")
    return ap.parse_args()

def main() -> None:
    args = parse_args()
    use_finnhub = args.provider in ("auto", "finnhub")
    if use_finnhub and not FINNHUB_API_KEY:
        raise SystemExit("FINNHUB_API_KEY is not set in the environment")
    end = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    start = end - dt.timedelta(days=args.lookback)
    start_ts = int(start.timestamp())
    end_ts = int(end.timestamp())
    start_iso = start.isoformat().replace("+00:00", "Z")
    end_iso = end.isoformat().replace("+00:00", "Z")

    conn = psycopg2.connect(db_url()); conn.autocommit = False
    try:
        slack = 3
        if args.use_table:
            scope_table = "alpaca_universe"
        else:
            scope_table = _load_temp_universe(conn, args.symbols_file)
        symbols = select_needy_symbols(conn, scope_table, args.lookback, slack, args.min_need, args.limit)
        print(f"[gaps] candidates: {len(symbols)} (relative lookback={args.lookback}d, slack={slack})")

        total_rows = 0
        for i, sym in enumerate(symbols, 1):
            try:
                wrote = 0
                used_source = None
                out = None
                if use_finnhub:
                    try:
                        out = finnhub_candles(sym, start_ts, end_ts)
                    except requests.HTTPError as exc:
                        status = getattr(exc.response, "status_code", None)
                        if status == 403 and args.provider == "auto":
                            out = None
                        else:
                            raise
                if out:
                    rows, last_price, avg_dollar_vol = out
                    wrote = upsert_returns(conn, rows)
                    total_rows += wrote
                    update_symbol_meta(conn, sym, last_price, avg_dollar_vol)
                    used_source = "finnhub"
                elif args.provider in ("auto", "alpaca"):
                    rows = alpaca_bars(sym, start_iso, end_iso)
                    if rows:
                        wrote = upsert_returns(conn, rows)
                        total_rows += wrote
                        used_source = "alpaca"
                    else:
                        print(f"[warn] {sym}: no rows from Alpaca bars")
                        used_source = None
                if used_source and i % 50 == 0:
                    print(f"[gaps] {i}/{len(symbols)} symbols via {used_source}; total rows={total_rows}")
            except Exception as exc:
                print(f"[warn] {sym}: {exc}")
            time.sleep(args.sleep)
        print(f"[gaps] complete: rows={total_rows} symbols={len(symbols)}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
