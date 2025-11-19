#!/usr/bin/env python3
from __future__ import annotations
import argparse, datetime as dt, os, sys, time
from typing import List, Dict, Iterable
import requests, psycopg2, psycopg2.extras as extras

ALP_DATA_BASE = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").rstrip("/")
ALP_DATA_FEED = os.getenv("ALPACA_DATA_FEED", "iex")
KEY = os.getenv("ALPACA_API_KEY_ID"); SEC = os.getenv("ALPACA_API_SECRET_KEY")
UNIVERSE_FILE = "/mnt/data/PulseTrade/db/alpaca_universe.symbols.txt"

def db_url()->str:
    return (os.getenv("DATABASE_URL")
            or f"postgresql://{os.getenv('POSTGRES_USER','pulse')}:{os.getenv('POSTGRES_PASSWORD','pulsepass')}"
               f"@{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','pulse')}")

def read_universe(path=UNIVERSE_FILE)->List[str]:
    with open(path,'r',encoding='utf-8') as f:
        out=[]; seen=set()
        for line in f:
            s=line.strip().upper()
            if s and not s.startswith('#') and s not in seen:
                seen.add(s); out.append(s)
        return out

def symbols_missing_ranges(conn, syms:List[str], min_start:dt.date)->Dict[str, dt.date]:
    # For each symbol, find the next date to fetch = max(existing ds)+1 or min_start
    sql = "SELECT symbol, MAX(ds) FROM daily_returns WHERE symbol = ANY(%s) GROUP BY symbol"
    start_by = {s:min_start for s in syms}
    with conn.cursor() as cur:
        cur.execute(sql, (syms,))
        for sym, maxds in cur.fetchall():
            if maxds and maxds >= min_start:
                start_by[sym] = maxds + dt.timedelta(days=1)
    return start_by

def chunked(seq:List[str], n:int)->Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def fetch_bars_batch(session:requests.Session, symbols:List[str], start_iso:str, end_iso:str):
    url = f"{ALP_DATA_BASE}/v2/stocks/bars"
    params = {
        "timeframe":"1Day",
        "symbols":",".join(symbols),
        "start": start_iso,
        "end": end_iso,
        "adjustment":"split",
        "feed": ALP_DATA_FEED,
        "limit": 10_000,
    }
    out=[]
    page_token=None
    while True:
        p = dict(params)
        if page_token: p["page_token"]=page_token
        r = session.get(url, params=p, timeout=30)
        if r.status_code==429:
            time.sleep(1.2); continue
        r.raise_for_status()
        j = r.json()
        bars = j.get("bars") or []
        # Normalize shape: list of dicts with symbol stored under "S"
        if isinstance(bars, dict):
            for sym, arr in bars.items():
                for b in arr or []:
                    if isinstance(b, dict):
                        bb = dict(b)
                        bb.setdefault("S", sym)
                        out.append(bb)
        elif isinstance(bars, list):
            out.extend([b for b in bars if isinstance(b, dict)])
        else:
            pass
        page_token = j.get("next_page_token")
        if not page_token:
            break
    return out

def _parse_bar_date(tval) -> dt.date | None:
    if tval is None:
        return None
    if isinstance(tval, str):
        s = tval.replace("Z", "+00:00")
        try:
            return dt.datetime.fromisoformat(s).date()
        except Exception:
            return None
    try:
        ns = int(tval)
        return dt.datetime.utcfromtimestamp(ns / 1e9).date()
    except Exception:
        return None

def to_daily_return_rows(bars:List[dict])->List[tuple]:
    rows=[]
    prev_by={}
    for b in bars:
        s = b.get("S") or b.get("T") or b.get("symbol")
        c=float(b.get("c") or 0.0); v=float(b.get("v") or 0.0)
        t=b.get("t")
        if not s or not t or c<=0:
            continue
        ds = _parse_bar_date(t)
        if not ds:
            continue
        dv = c*v
        prev = prev_by.get(s)
        if prev and prev>0:
            import math
            y = math.log(c/prev)
            rows.append((ds, s, y, dv))
        prev_by[s]=c
    return rows

def upsert_rows(conn, rows:List[tuple]):
    if not rows: return 0
    with conn.cursor() as cur:
        extras.execute_batch(cur, """
            INSERT INTO daily_returns (ds, symbol, y, dollar_vol)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (symbol, ds)
            DO UPDATE SET y=EXCLUDED.y, dollar_vol=EXCLUDED.dollar_vol
        """, rows, page_size=2000)
    conn.commit()
    return len(rows)

def main():
    ap=argparse.ArgumentParser(description="Bulk bars backfill for entire Alpaca universe (no news).")
    ap.add_argument("--days", type=int, default=int(os.getenv("BIG_BACKFILL_DAYS","120")))
    ap.add_argument("--batch", type=int, default=int(os.getenv("ALPACA_BARS_BATCH","100")))
    ap.add_argument("--sleep", type=float, default=float(os.getenv("ALPACA_BARS_BATCH_SLEEP","0.5")))
    args=ap.parse_args()
    if not KEY or not SEC: sys.exit("Missing ALPACA_API_KEY_ID/ALPACA_API_SECRET_KEY")

    session=requests.Session()
    session.headers.update({"APCA-API-KEY-ID":KEY, "APCA-API-SECRET-KEY":SEC, "Accept":"application/json"})
    conn=psycopg2.connect(db_url()); conn.autocommit=False

    try:
        universe = read_universe()
        # Use UTC timestamps for Alpaca start/end
        end_dt = dt.datetime.now(dt.timezone.utc)
        end_iso = end_dt.isoformat().replace("+00:00", "Z")
        start_floor = end_dt.date() - dt.timedelta(days=args.days)
        starts = symbols_missing_ranges(conn, universe, start_floor)

        # Group symbols by identical start date to reduce distinct requests
        bucket: Dict[dt.date, List[str]] = {}
        for s, st in starts.items(): bucket.setdefault(st, []).append(s)

        total=0
        for st, syms in bucket.items():
            start_dt = dt.datetime.combine(st, dt.time(0, 0, tzinfo=dt.timezone.utc))
            if start_dt >= end_dt:
                continue
            start_iso = start_dt.isoformat().replace("+00:00", "Z")
            for group in chunked(sorted(syms), args.batch):
                bars = fetch_bars_batch(session, group, start_iso, end_iso)
                rows = to_daily_return_rows(bars)
                total += upsert_rows(conn, rows)
                if args.sleep>0: time.sleep(args.sleep)
        print(f"[ok] Backfill complete: inserted/updated {total} daily_return rows")
    finally:
        conn.close()

if __name__=="__main__":
    main()
