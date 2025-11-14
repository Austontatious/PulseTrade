import os
import uuid
import time
from typing import Optional, Dict, Any, List

import httpx

ALPACA_ENABLED = os.getenv("ENABLE_ALPACA_EXECUTOR", "0") == "1"
ALPACA_BASE = os.getenv("ALPACA_PAPER_BASE_URL", "https://paper-api.alpaca.markets")
ALPACA_KEY = os.getenv("ALPACA_API_KEY_ID")
ALPACA_SECRET = os.getenv("ALPACA_API_SECRET_KEY")
ALPACA_NOTIONAL = os.getenv("ALPACA_ORDER_NOTIONAL")  # optional dollar notional
ALPACA_TIF = os.getenv("ALPACA_TIME_IN_FORCE", "gtc")
ALPACA_ORDER_TYPE = os.getenv("ALPACA_ORDER_TYPE", "market")

# Simple token-bucket rate limiters
_ORD_CAP = int(os.getenv("ALPACA_ORDERS_PER_SEC", "2"))
_POS_CAP = int(os.getenv("ALPACA_POS_PER_SEC", "5"))
_ord_tokens = _ORD_CAP
_ord_last = time.monotonic()
_pos_tokens = _POS_CAP
_pos_last = time.monotonic()

async def _acquire(kind: str) -> None:
    # Cooperative async sleep based rate limiter
    global _ord_tokens, _ord_last, _pos_tokens, _pos_last
    cap = _ORD_CAP if kind == 'ord' else _POS_CAP
    while True:
        now = time.monotonic()
        if kind == 'ord':
            elapsed = now - _ord_last
            add = int(elapsed * _ORD_CAP)
            if add > 0:
                _ord_tokens = min(cap, _ord_tokens + add)
                _ord_last = now
            if _ord_tokens > 0:
                _ord_tokens -= 1
                return
        else:
            elapsed = now - _pos_last
            add = int(elapsed * _POS_CAP)
            if add > 0:
                _pos_tokens = min(cap, _pos_tokens + add)
                _pos_last = now
            if _pos_tokens > 0:
                _pos_tokens -= 1
                return
        # No tokens, sleep a bit
        import asyncio
        await asyncio.sleep(0.1)

def _orders_url() -> str:
    base = (ALPACA_BASE or "https://paper-api.alpaca.markets").rstrip("/")
    # Allow either base like https://.../ (we'll append /v2/orders)
    # or base already including /v2
    if base.endswith("/v2"):
        return f"{base}/orders"
    return f"{base}/v2/orders"

def _positions_url(symbol: str) -> str:
    base = (ALPACA_BASE or "https://paper-api.alpaca.markets").rstrip("/")
    if base.endswith("/v2"):
        return f"{base}/positions/{symbol}"
    return f"{base}/v2/positions/{symbol}"

async def submit_order(
    symbol: str,
    side: str,
    qty: Optional[float] = None,
    *,
    order_type: Optional[str] = None,
    time_in_force: Optional[str] = None,
    notional: Optional[float] = None,
    limit_price: Optional[float] = None,
    order_class: Optional[str] = None,
    take_profit_limit: Optional[float] = None,
    stop_loss_stop: Optional[float] = None,
    stop_loss_limit: Optional[float] = None,
    stop_price: Optional[float] = None,
) -> Optional[dict]:
    url = _orders_url()
    otype = (order_type or ALPACA_ORDER_TYPE).lower()
    tif = (time_in_force or ALPACA_TIF).lower()
    payload: Dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "type": otype,
        "time_in_force": tif,
        "client_order_id": str(uuid.uuid4()),
    }
    if otype == "limit":
        # Alpaca requires qty for limit orders; notional is not supported for limit
        if qty is None or limit_price is None:
            raise ValueError("limit orders require qty and limit_price")
        payload["qty"] = str(qty)
        payload["limit_price"] = float(limit_price)
    elif otype == "stop":
        if qty is None or stop_price is None:
            raise ValueError("stop orders require qty and stop_price")
        payload["qty"] = str(qty)
        payload["stop_price"] = float(stop_price)
    else:
        # Market order: prefer caller-provided qty if available, otherwise fall back to notional.
        if qty is not None:
            payload["qty"] = str(qty)
        elif notional is not None:
            payload["notional"] = float(notional)
        elif ALPACA_NOTIONAL and ALPACA_NOTIONAL.strip() and float(ALPACA_NOTIONAL) > 0:
            payload["notional"] = float(ALPACA_NOTIONAL)
        else:
            raise ValueError("market orders require qty or notional")
    if order_class:
        payload["order_class"] = order_class
        if take_profit_limit is not None:
            payload["take_profit"] = {"limit_price": float(take_profit_limit)}
        if stop_loss_stop is not None:
            sl: Dict[str, Any] = {"stop_price": float(stop_loss_stop)}
            if stop_loss_limit is not None:
                sl["limit_price"] = float(stop_loss_limit)
            payload["stop_loss"] = sl

    headers = {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
        "Content-Type": "application/json",
    }
    await _acquire('ord')
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(url, json=payload, headers=headers)
        except Exception as exc:
            print("alpaca submit error:", exc)
            raise
        if resp.status_code >= 400:
            try:
                print("alpaca order error:", resp.status_code, resp.text, flush=True)
                try:
                    body = resp.request.content.decode() if resp.request.content else ""
                except Exception:
                    body = "<unable to decode>"
                print("alpaca order payload:", payload, flush=True)
                print("alpaca order serialized:", body, flush=True)
            except Exception:
                pass
            resp.raise_for_status()
        return resp.json()

async def get_position_qty(symbol: str) -> float:
    if not ALPACA_KEY or not ALPACA_SECRET:
        return 0.0
    url = _positions_url(symbol)
    headers = {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }
    await _acquire('pos')
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code == 404:
            return 0.0
        if resp.status_code >= 400:
            try:
                print("alpaca positions error:", resp.status_code, resp.text)
            except Exception:
                pass
            return 0.0
        data = resp.json()
        # qty field is a string according to Alpaca docs
        qty_str = data.get("qty") or data.get("qty_available") or "0"
        try:
            return float(qty_str)
        except Exception:
            return 0.0

async def get_all_positions() -> List[Dict[str, Any]]:
    """Fetch all open positions from Alpaca Paper API.

    Returns an empty list if credentials are missing or request fails.
    """
    if not ALPACA_KEY or not ALPACA_SECRET:
        return []
    base = (ALPACA_BASE or "https://paper-api.alpaca.markets").rstrip("/")
    url = f"{base}/v2/positions"
    headers = {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }
    await _acquire('pos')
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(url, headers=headers)
            if resp.status_code >= 400:
                try:
                    print("alpaca positions list error:", resp.status_code, resp.text)
                except Exception:
                    pass
                return []
            data = resp.json()
            if isinstance(data, list):
                return data  # type: ignore[return-value]
            return []
        except Exception:
            return []

async def close_position(symbol: str) -> Optional[Dict[str, Any]]:
    """Close the entire position for the given symbol via Alpaca.

    Uses DELETE /v2/positions/{symbol}. Returns response JSON or None on failure.
    """
    if not ALPACA_KEY or not ALPACA_SECRET:
        return None
    base = (ALPACA_BASE or "https://paper-api.alpaca.markets").rstrip("/")
    url = f"{base}/v2/positions/{symbol}"
    headers = {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }
    await _acquire('pos')
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.delete(url, headers=headers)
            if resp.status_code >= 400:
                try:
                    print("alpaca close position error:", resp.status_code, resp.text)
                except Exception:
                    pass
                return None
            try:
                return resp.json()
            except Exception:
                return {}
        except Exception:
            return None


async def get_account() -> Dict[str, Any]:
    """Fetch Alpaca account snapshot."""
    if not ALPACA_KEY or not ALPACA_SECRET:
        return {}
    base = (ALPACA_BASE or "https://paper-api.alpaca.markets").rstrip("/")
    url = f"{base}/v2/account"
    headers = {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }
    await _acquire('ord')
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.get(url, headers=headers)
            if resp.status_code >= 400:
                try:
                    print("alpaca account error:", resp.status_code, resp.text)
                except Exception:
                    pass
                return {}
            data = resp.json()
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

# Open orders (dedupe)
_open_cache: Dict[str, Dict[str, float]] = {}

def _orders_list_url() -> str:
    base = (ALPACA_BASE or "https://paper-api.alpaca.markets").rstrip("/")
    if base.endswith("/v2"):
        return f"{base}/orders"
    return f"{base}/v2/orders"

async def get_open_orders(symbol: str) -> List[Dict[str, Any]]:
    # Return cached results if younger than 10s
    now = time.monotonic()
    cache_key = f"{symbol}"
    meta = _open_cache.get(cache_key)
    if meta and now - meta.get("ts", 0) < 10:
        return meta.get("data", [])  # type: ignore
    headers = {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }
    params = {"status": "open", "limit": 200}
    await _acquire('ord')
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(_orders_list_url(), headers=headers, params=params)
        if resp.status_code >= 400:
            try:
                print("alpaca open orders error:", resp.status_code, resp.text)
            except Exception:
                pass
            data: List[Dict[str, Any]] = []
        else:
            all_orders = resp.json()
            data = [o for o in all_orders if o.get("symbol") == symbol and o.get("status") == "open"]
    _open_cache[cache_key] = {"ts": now, "data": data}
    return data

async def latest_quote(ticker: str) -> Optional[Dict[str, Any]]:
    # This executor-level quote fetch uses DB to keep it simple
    import asyncpg
    import datetime as dt
    DB_DSN = (
        f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
        f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'pulse')}"
    )
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        row = await conn.fetchrow(
            "SELECT ts, bid, ask FROM quotes WHERE ticker=$1 ORDER BY ts DESC LIMIT 1",
            ticker,
        )
        if not row:
            return None
        return {"ts": row["ts"], "bid": row["bid"], "ask": row["ask"]}
    finally:
        await conn.close()

async def maybe_submit_order(
    symbol: str,
    side: str,
    qty: float,
    limit_price: Optional[float] = None,
    order_type: Optional[str] = None,
    *,
    order_class: Optional[str] = None,
    take_profit_limit: Optional[float] = None,
    stop_loss_stop: Optional[float] = None,
    stop_loss_limit: Optional[float] = None,
    stop_price: Optional[float] = None,
    use_notional_for_market: bool = True,
) -> Optional[dict]:
    if not ALPACA_ENABLED or not ALPACA_KEY or not ALPACA_SECRET:
        return None
    try:
        # For market orders, allow callers to bypass notional usage and force qty sizing
        return await submit_order(
            symbol,
            side,
            qty,
            order_type=order_type or ALPACA_ORDER_TYPE,
            time_in_force=ALPACA_TIF,
            notional=(
                None if (order_type or ALPACA_ORDER_TYPE).lower() == "limit"
                else (float(ALPACA_NOTIONAL) if (use_notional_for_market and ALPACA_NOTIONAL) else None)
            ),
            limit_price=limit_price,
            order_class=order_class,
            take_profit_limit=take_profit_limit,
            stop_loss_stop=stop_loss_stop,
            stop_loss_limit=stop_loss_limit,
            stop_price=stop_price,
        )
    except Exception as exc:  # pragma: no cover - network failure logging
        print("alpaca submit error:", exc)
        return None
