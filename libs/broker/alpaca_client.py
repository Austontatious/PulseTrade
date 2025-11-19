from __future__ import annotations

import os
from typing import Any, Dict

import httpx


ALPACA_BASE = os.getenv("ALPACA_PAPER_BASE_URL", "https://paper-api.alpaca.markets").rstrip("/")
ALPACA_KEY = os.getenv("ALPACA_API_KEY_ID")
ALPACA_SECRET = os.getenv("ALPACA_API_SECRET_KEY")
DEFAULT_TIF = os.getenv("ALPACA_TIME_IN_FORCE", "day")


def _require_creds() -> None:
    if not ALPACA_KEY or not ALPACA_SECRET:
        raise RuntimeError("Alpaca credentials are missing")


def _url(path: str) -> str:
    if path.startswith("http"):
        return path
    base = (ALPACA_BASE or "https://paper-api.alpaca.markets").rstrip("/")
    clean_path = path if path.startswith("/") else f"/{path}"
    if base.endswith("/v2"):
        return f"{base}{clean_path}"
    return f"{base}/v2{clean_path}"


def _headers() -> Dict[str, str]:
    _require_creds()
    return {
        "APCA-API-KEY-ID": ALPACA_KEY or "",
        "APCA-API-SECRET-KEY": ALPACA_SECRET or "",
        "Content-Type": "application/json",
    }


def _request(method: str, path: str, **kwargs) -> Dict[str, Any]:
    url = _url(path)
    headers = kwargs.pop("headers", {})
    headers.update(_headers())
    with httpx.Client(timeout=10.0) as client:
        resp = client.request(method, url, headers=headers, **kwargs)
        resp.raise_for_status()
        if not resp.content:
            return {}
        data = resp.json()
        if isinstance(data, dict):
            return data
        return {"data": data}


def get_asset(symbol: str) -> Dict[str, Any]:
    return _request("GET", f"/assets/{symbol.upper()}")


def is_shortable(symbol: str) -> bool:
    try:
        info = get_asset(symbol)
    except Exception:
        return False
    return bool(info.get("shortable")) and bool(info.get("easy_to_borrow"))


def place_short_sell(symbol: str, qty: int, tif: str | None = None) -> Dict[str, Any]:
    if qty <= 0:
        raise ValueError("qty must be positive for short sells")
    payload = {
        "symbol": symbol.upper(),
        "qty": str(qty),
        "side": "sell",
        "type": "market",
        "time_in_force": (tif or DEFAULT_TIF or "day"),
    }
    return _request("POST", "/orders", json=payload)


def buy_to_cover(symbol: str, qty: int, tif: str | None = None) -> Dict[str, Any]:
    if qty <= 0:
        raise ValueError("qty must be positive for buy-to-cover")
    payload = {
        "symbol": symbol.upper(),
        "qty": str(qty),
        "side": "buy",
        "type": "market",
        "time_in_force": (tif or DEFAULT_TIF or "day"),
    }
    return _request("POST", "/orders", json=payload)


def trailing_buy_to_cover(symbol: str, trail_percent: float, qty: int, tif: str | None = None) -> Dict[str, Any]:
    if qty <= 0:
        raise ValueError("qty must be positive for trailing buy-to-cover")
    payload = {
        "symbol": symbol.upper(),
        "qty": str(qty),
        "side": "buy",
        "type": "trailing_stop",
        "trail_percent": round(trail_percent * 100.0, 4),
        "time_in_force": (tif or DEFAULT_TIF or "day"),
    }
    return _request("POST", "/orders", json=payload)


def get_position(symbol: str) -> Dict[str, Any]:
    """Return the raw Alpaca position payload for the given symbol."""
    try:
        return _request("GET", f"/positions/{symbol.upper()}")
    except httpx.HTTPStatusError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return {}
        raise
