import os
import json
import asyncio
import datetime as dt
import websockets
import asyncpg
from .config import DB_DSN

API_KEY = os.getenv("ALPACA_API_KEY_ID")
API_SECRET = os.getenv("ALPACA_API_SECRET_KEY")
SYMBOLS = [s.strip() for s in os.getenv("ALPACA_SYMBOLS", "AAPL,MSFT,SPY").split(",") if s.strip()]
WS_REFRESH_SECS = int(os.getenv("ALPACA_WS_REFRESH_SECS", "60"))
WS_MAX_SUB = int(os.getenv("ALPACA_WS_MAX_SUB", "200"))

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
    while True:
        try:
            print(f"alpaca connecting[{label}]: {url} symbols={SYMBOLS}")
            async with websockets.connect(url, ping_interval=20) as ws:
                await ws.send(json.dumps({"action": "auth", "key": API_KEY, "secret": API_SECRET}))
                print(f"alpaca auth sent [{label}]")
                await ws.send(json.dumps({"action": "subscribe", "trades": SYMBOLS}))
                print(f"alpaca subscribe sent [{label}]")
                conn = await asyncpg.connect(dsn=DB_DSN)
                try:
                    dbg = 0
                    subscribed = set(SYMBOLS)

                    async def refresh_symbols():
                        while True:
                            try:
                                rows = await conn.fetch("SELECT ticker FROM symbols WHERE class='equity' ORDER BY ticker ASC LIMIT 1000")
                                dyn = [r[0] for r in rows]
                                merged = list({*(t.upper().replace('.', '-') for t in dyn if t and ' ' not in t), *subscribed})
                                # Respect max subscription size
                                need = [t for t in merged if t not in subscribed]
                                if need:
                                    # cap additions to reach WS_MAX_SUB
                                    add_room = max(0, WS_MAX_SUB - len(subscribed))
                                    to_add = need[:add_room]
                                    if to_add:
                                        await ws.send(json.dumps({"action": "subscribe", "trades": to_add}))
                                        for s in to_add:
                                            subscribed.add(s)
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
