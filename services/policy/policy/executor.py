import os
import uuid
from typing import Optional

import httpx

ALPACA_ENABLED = os.getenv("ENABLE_ALPACA_EXECUTOR", "0") == "1"
ALPACA_BASE = os.getenv("ALPACA_PAPER_BASE_URL", "https://paper-api.alpaca.markets")
ALPACA_KEY = os.getenv("ALPACA_API_KEY_ID")
ALPACA_SECRET = os.getenv("ALPACA_API_SECRET_KEY")

async def submit_order(symbol: str, side: str, qty: float, price: float) -> Optional[dict]:
    url = f"{ALPACA_BASE.rstrip('/')}/v2/orders"
    payload = {
        "symbol": symbol,
        "qty": qty,
        "side": side,
        "type": "market",
        "time_in_force": "gtc",
        "client_order_id": str(uuid.uuid4()),
    }
    headers = {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        return resp.json()

async def maybe_submit_order(symbol: str, side: str, qty: float, price: float) -> Optional[dict]:
    if not ALPACA_ENABLED or not ALPACA_KEY or not ALPACA_SECRET:
        return None
    try:
        return await submit_order(symbol, side, qty, price)
    except Exception as exc:  # pragma: no cover - network failure logging
        print("alpaca submit error:", exc)
        return None
