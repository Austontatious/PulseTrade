import os
import json
import asyncio
import datetime as dt
from pathlib import Path
import websockets
import asyncpg
from .config import DB_DSN

API_KEY = os.getenv("ALPACA_API_KEY_ID")
API_SECRET = os.getenv("ALPACA_API_SECRET_KEY")
ENV_SYMBOLS = [s.strip() for s in os.getenv("ALPACA_SYMBOLS", "AAPL,MSFT,SPY").split(",") if s.strip()]
WS_REFRESH_SECS = int(os.getenv("ALPACA_WS_REFRESH_SECS", "60"))
WS_MAX_SUB = int(os.getenv("ALPACA_WS_MAX_SUB", "200"))
SUB_CHUNK = max(1, int(os.getenv("ALPACA_WS_SUB_CHUNK", "100")))
SUB_PAUSE_S = float(os.getenv("ALPACA_WS_SUB_PAUSE_S", "1.0"))
WS_DISABLE = os.getenv("ALPACA_WS_DISABLE", "0") == "1"
WS_DISABLE_EXTRA = os.getenv("ALPACA_WS_DISABLE_EXTRA_LISTS", "1") == "1"
UNIVERSE_FILE = Path(os.getenv("ALPACA_UNIVERSE_FILE") or (Path(__file__).resolve().parents[1] / "universe_symbols.txt"))


def _normalise_symbol(raw: str) -> str | None:
    if not raw:
        return None
    sym = raw.strip().upper().replace(".", "-")
    if not sym or " " in sym or len(sym) > 7:
        return None
    return sym


def _load_universe_symbols() -> list[str]:
    symbols: list[str] = []
    if UNIVERSE_FILE.exists():
        try:
            for line in UNIVERSE_FILE.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                sym = _normalise_symbol(line)
                if sym:
                    symbols.append(sym)
        except Exception as exc:
            print("alpaca ws universe read error:", exc)
    if not symbols:
        for raw in ENV_SYMBOLS:
            sym = _normalise_symbol(raw)
            if sym:
                symbols.append(sym)
    unique = list(dict.fromkeys(symbols))
    if WS_MAX_SUB > 0:
        unique = unique[:WS_MAX_SUB]
    return unique


async def _subscribe_chunks(ws, symbols: list[str]) -> None:
    if not symbols:
        return
    batch = list(dict.fromkeys(symbols))
    for i in range(0, len(batch), SUB_CHUNK):
        chunk = batch[i : i + SUB_CHUNK]
        payload = {"action": "subscribe", "trades": chunk, "quotes": chunk}
        await ws.send(json.dumps(payload))
        if SUB_PAUSE_S > 0:
            await asyncio.sleep(SUB_PAUSE_S)

def _parse_ts(tval):
    ts = None
    if isinstance(tval, (int, float)):
        ts = dt.datetime.fromtimestamp(float(tval) / 1e9, tz=dt.timezone.utc)
    elif isinstance(tval, str):
        # Try epoch ns string
        try:
            ts = dt.datetime.fromtimestamp(float(int(tval)) / 1e9, tz=dt.timezone.utc)
        except Exception:
            # Handle RFC3339 with nanoseconds by trimming to microseconds
            s = tval.replace("Z", "+00:00")
            if "." in s:
                head, tail = s.split(".", 1)
                tz_index = max(tail.find("+"), tail.find("-"))
                if tz_index != -1:
                    frac = tail[:tz_index]
                    tz = tail[tz_index:]
                else:
                    frac, tz = tail, ""
                frac = ''.join(ch for ch in frac if ch.isdigit())[:6]
                s2 = f"{head}.{frac}{tz}" if frac else f"{head}{tz}"
            else:
                s2 = s
            try:
                ts = dt.datetime.fromisoformat(s2)
            except Exception:
                ts = None
    return ts

async def _ingest_ws(url: str, label: str) -> None:
    if not API_KEY or not API_SECRET:
        print(f"alpaca {label} disabled (missing keys)")
        return
    if WS_DISABLE:
        print("alpaca ws disabled via ALPACA_WS_DISABLE=1")
        return
    while True:
        try:
            symbols = _load_universe_symbols()
            if not symbols:
                print("alpaca ws has no universe symbols; sleeping 5s")
                await asyncio.sleep(5)
                continue
            print(f"alpaca connecting[{label}]: {url} symbols={len(symbols)}")
            async with websockets.connect(url, ping_interval=20) as ws:
                await ws.send(json.dumps({"action": "auth", "key": API_KEY, "secret": API_SECRET}))
                print(f"alpaca auth sent [{label}]")
                await _subscribe_chunks(ws, symbols)
                print(f"alpaca subscribe sent [{label}] (trades+quotes, {len(symbols)} symbols)")
                conn = await asyncpg.connect(dsn=DB_DSN)
                try:
                    dbg = 0
                    subscribed = set(symbols)

                    async def refresh_symbols():
                        while True:
                            try:
                                desired = _load_universe_symbols()
                                if not WS_DISABLE_EXTRA:
                                    rows = await conn.fetch(
                                        "SELECT ticker FROM symbols WHERE class='equity' ORDER BY ticker ASC LIMIT 1000"
                                    )
                                    dyn = [
                                        _normalise_symbol(r[0])
                                        for r in rows
                                    ]
                                    desired.extend(sym for sym in dyn if sym)
                                desired = list(dict.fromkeys(desired))
                                if WS_MAX_SUB > 0:
                                    desired = desired[:WS_MAX_SUB]
                                additions = [s for s in desired if s not in subscribed]
                                if additions:
                                    add_room = max(0, WS_MAX_SUB - len(subscribed)) if WS_MAX_SUB > 0 else len(additions)
                                    to_add = additions[:add_room] if add_room else []
                                    if to_add:
                                        await _subscribe_chunks(ws, to_add)
                                        subscribed.update(to_add)
                                        print(f"alpaca subscribed {len(to_add)} new [{label}]")
                            except Exception as e:
                                try:
                                    print(f"alpaca ws refresh error[{label}]:", e)
                                except Exception:
                                    pass
                            await asyncio.sleep(WS_REFRESH_SECS)

                    # Start background refresh
                    refresh_task = asyncio.create_task(refresh_symbols())
                    while True:
                        message = await ws.recv()
                        payload = json.loads(message)
                        if isinstance(payload, list):
                            for event in payload:
                                etype = event.get("T")
                                if etype == "t":
                                    ts = _parse_ts(event.get("t"))
                                    if ts is None:
                                        continue
                                    price = float(event.get("p", 0.0))
                                    size = float(event.get("s") or 0.0)
                                    await conn.execute(
                                        "INSERT INTO trades(ts,ticker,price,size,venue,meta) VALUES($1,$2,$3,$4,$5,$6)",
                                        ts,
                                        event.get("S"),
                                        price,
                                        size,
                                        f"ALPACA_{label.upper()}",
                                        json.dumps(event),
                                    )
                                elif etype == "q":
                                    # Quotes
                                    tval = event.get("t")
                                    ts = _parse_ts(tval)
                                    if ts is None:
                                        continue
                                    bid = float(event.get("bp") or 0.0)
                                    ask = float(event.get("ap") or 0.0)
                                    await conn.execute(
                                        "INSERT INTO quotes(ts,ticker,bid,ask,venue,meta) VALUES($1,$2,$3,$4,$5,$6)",
                                        ts,
                                        event.get("S"),
                                        bid,
                                        ask,
                                        f"ALPACA_{label.upper()}",
                                        json.dumps(event),
                                    )
                                else:
                                    if dbg < 5:
                                        try:
                                            print(f"alpaca ws non-trade[{label}]:", event)
                                        except Exception:
                                            pass
                                        dbg += 1
                        else:
                            try:
                                print(f"alpaca ws ctrl[{label}]:", payload)
                            except Exception:
                                pass
                except Exception:
                    raise
                finally:
                    try:
                        refresh_task.cancel()
                    except Exception:
                        pass
                    await conn.close()
        except Exception as exc:  # pragma: no cover - network loop guard
            print(f"alpaca ws error[{label}]:", exc)
            await asyncio.sleep(3)

async def run_alpaca() -> None:
    if WS_DISABLE:
        print("alpaca ws disabled (ALPACA_WS_DISABLE=1)")
        return
    # Enable both feeds concurrently when desired
    enable_iex = os.getenv("ENABLE_ALPACA_IEX_WS", "1") == "1"
    enable_sip = os.getenv("ENABLE_ALPACA_SIP_WS", "1") == "1"
    tasks = []
    if enable_iex:
        tasks.append(asyncio.create_task(_ingest_ws("wss://stream.data.alpaca.markets/v2/iex", "iex")))
    if enable_sip:
        tasks.append(asyncio.create_task(_ingest_ws("wss://stream.data.alpaca.markets/v2/sip", "sip")))
    if tasks:
        await asyncio.gather(*tasks)
