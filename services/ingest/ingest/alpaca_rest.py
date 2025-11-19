import os
import asyncio
import datetime as dt
from typing import List
import random
import httpx
import asyncpg
from pathlib import Path
from dotenv import load_dotenv

try:
    from services.ingest.config import DB_DSN
except ImportError:  # running as script, fall back to local module
    from config import DB_DSN  # type: ignore

_DOTENV_PATH = next((parent / ".env" for parent in Path(__file__).resolve().parents if (parent / ".env").exists()), None)
if _DOTENV_PATH:
    load_dotenv(dotenv_path=_DOTENV_PATH, override=False)

API_KEY = os.getenv("ALPACA_API_KEY_ID")
API_SECRET = os.getenv("ALPACA_API_SECRET_KEY")
BASELINE_SYMBOLS = [s.strip() for s in os.getenv("ALPACA_SYMBOLS", "AAPL,MSFT,SPY").split(",") if s.strip()]
RPM = int(os.getenv("ALPACA_REST_RPM", "120"))
BATCH = int(os.getenv("ALPACA_REST_BATCH", "50"))
BATCH_PAUSE_S = float(os.getenv("ALPACA_REST_BATCH_PAUSE_S", "2.0"))
PER_REQ_SLEEP = 60.0 / max(1, RPM)
MAX_BACKOFF = float(os.getenv("ALPACA_REST_MAX_BACKOFF_S", "30.0"))

def _headers():
    return {
        "APCA-API-KEY-ID": API_KEY or "",
        "APCA-API-SECRET-KEY": API_SECRET or "",
    }


async def _throttle_sleep(counter: int) -> None:
    if PER_REQ_SLEEP > 0:
        await asyncio.sleep(PER_REQ_SLEEP)
    if BATCH > 0 and counter % BATCH == 0 and BATCH_PAUSE_S > 0:
        await asyncio.sleep(BATCH_PAUSE_S)


async def _request_with_backoff(client: httpx.AsyncClient, url: str, params: dict) -> httpx.Response:
    sleep_s = max(PER_REQ_SLEEP, 0.0)
    while True:
        resp = await client.get(url, headers=_headers(), params=params, timeout=10)
        if resp.status_code != 429:
            return resp
        await asyncio.sleep(max(sleep_s, 0.5))
        sleep_s = min(MAX_BACKOFF, sleep_s * 2 if sleep_s else 1.0)

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
    r = await _request_with_backoff(client, url, params)
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
                for idx, sym in enumerate(symbols, 1):
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
                        await _throttle_sleep(idx)
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
                    for idx, sym in enumerate(symbols, 1):
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
                        finally:
                            await _throttle_sleep(idx)
                await asyncio.sleep(interval)
    finally:
        await conn.close()
