import os
import asyncio
import datetime as dt
from typing import List
import random
import httpx
import asyncpg
from .config import DB_DSN

API_KEY = os.getenv("ALPACA_API_KEY_ID")
API_SECRET = os.getenv("ALPACA_API_SECRET_KEY")
BASELINE_SYMBOLS = [s.strip() for s in os.getenv("ALPACA_SYMBOLS", "AAPL,MSFT,SPY").split(",") if s.strip()]

def _headers():
    return {
        "APCA-API-KEY-ID": API_KEY or "",
        "APCA-API-SECRET-KEY": API_SECRET or "",
    }

def _parse_ts(val: str | int | float | None) -> dt.datetime | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        # Latest trade returns RFC3339 string normally, but guard
        return dt.datetime.fromtimestamp(float(val) / 1e9, tz=dt.timezone.utc)
    if isinstance(val, str):
        s = val.replace("Z", "+00:00")
        # Handle RFC3339 with nanoseconds by trimming to microseconds
        if "." in s:
            head, tail = s.split(".", 1)
            # tail contains frac + timezone, split at '+' or '-'
            tz_index = max(tail.find("+"), tail.find("-"))
            if tz_index != -1:
                frac = tail[:tz_index]
                tz = tail[tz_index:]
            else:
                frac, tz = tail, ""
            frac = ''.join(ch for ch in frac if ch.isdigit())[:6]  # microseconds
            s2 = f"{head}.{frac}{tz}" if frac else f"{head}{tz}"
            try:
                return dt.datetime.fromisoformat(s2)
            except Exception:
                pass
        # Fallback: try integer epoch ns encoded as string
        try:
            return dt.datetime.fromtimestamp(float(int(val)) / 1e9, tz=dt.timezone.utc)
        except Exception:
            return None
    return None

async def _fetch_latest(symbol: str, feed: str, client: httpx.AsyncClient) -> tuple[dt.datetime | None, float | None]:
    # Use the per-symbol endpoint to simplify parsing
    url = f"https://data.alpaca.markets/v2/stocks/{symbol}/trades/latest"
    params = {"feed": feed}
    r = await client.get(url, headers=_headers(), params=params, timeout=10)
    if r.status_code != 200:
        try:
            print("alpaca REST status", r.status_code, url, r.text[:200])
        except Exception:
            pass
        return None, None
    data = r.json()
    trade = data.get("trade") or {}
    ts = _parse_ts(trade.get("t"))
    price = trade.get("p")
    try:
        price = float(price) if price is not None else None
    except Exception:
        price = None
    return ts, price

async def _load_symbols(conn: asyncpg.Connection) -> list[str]:
    rows = await conn.fetch("SELECT ticker FROM symbols WHERE class='equity'")
    dyn = [r[0] for r in rows]
    # union of baseline env and dynamic DB symbols
    merged = list({*BASELINE_SYMBOLS, *dyn})
    # filter invalid
    clean = []
    for s in merged:
        s2 = (s or '').strip().upper().replace('.', '-')
        if s2 and ' ' not in s2 and 1 <= len(s2) <= 7:
            clean.append(s2)
    return list(dict.fromkeys(clean))

async def backfill_once(feeds: List[str] | None = None, max_symbols: int | None = None) -> int:
    if not API_KEY or not API_SECRET:
        return 0
    feeds_to_use = list(feeds) if feeds else []
    if not feeds_to_use:
        if os.getenv("ENABLE_ALPACA_IEX_REST", "1") == "1":
            feeds_to_use.append("iex")
        if os.getenv("ENABLE_ALPACA_SIP_REST", "0") == "1":
            feeds_to_use.append("sip")
    if not feeds_to_use:
        return 0
    conn = await asyncpg.connect(dsn=DB_DSN)
    inserted = 0
    try:
        symbols = await _load_symbols(conn)
        if max_symbols:
            symbols = symbols[:max_symbols]
        async with httpx.AsyncClient() as client:
            for feed in feeds_to_use:
                for sym in symbols:
                    try:
                        ts, price = await _fetch_latest(sym, feed, client)
                        if ts and price:
                            await conn.execute(
                                "INSERT INTO trades(ts,ticker,price,size,venue,meta) VALUES($1,$2,$3,$4,$5,$6)",
                                ts,
                                sym,
                                price,
                                None,
                                f"ALPACA_REST_{feed.upper()}",
                                None,
                            )
                            inserted += 1
                    except Exception as exc:
                        print("alpaca REST backfill error:", sym, feed, exc)
    finally:
        await conn.close()
    return inserted


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        total = asyncio.run(backfill_once())
        print(f"alpaca REST backfill inserted {total} rows")

async def run_alpaca_rest_poller() -> None:
    if not API_KEY or not API_SECRET:
        print("alpaca REST disabled (missing keys)")
        return
    interval = int(os.getenv("ALPACA_REST_POLL_SECS", "30"))
    feeds: List[str] = []
    if os.getenv("ENABLE_ALPACA_IEX_REST", "1") == "1":
        feeds.append("iex")
    if os.getenv("ENABLE_ALPACA_SIP_REST", "0") == "1":
        feeds.append("sip")
    if not feeds:
        return
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        async with httpx.AsyncClient() as client:
            while True:
                symbols = await _load_symbols(conn)
                random.shuffle(symbols)
                max_per = int(os.getenv("ALPACA_REST_MAX_PER_CYCLE", "50"))
                symbols = symbols[:max_per]
                for feed in feeds:
                    for sym in symbols:
                        try:
                            ts, price = await _fetch_latest(sym, feed, client)
                            if ts and price:
                                await conn.execute(
                                    "INSERT INTO trades(ts,ticker,price,size,venue,meta) VALUES($1,$2,$3,$4,$5,$6)",
                                    ts,
                                    sym,
                                    price,
                                    None,
                                    f"ALPACA_REST_{feed.upper()}",
                                    None,
                                )
                                print("alpaca REST inserted", sym, feed, ts, price)
                        except Exception as e:
                            print("alpaca REST poll error:", sym, feed, e)
                await asyncio.sleep(interval)
    finally:
        await conn.close()
