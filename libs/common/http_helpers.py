from __future__ import annotations
import asyncio
import time
from typing import Optional, Dict, Any
import httpx

class RateLimiter:
    """Simple async rate limiter enforcing max requests per second."""

    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(1e-9, rps)
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self.min_interval - (now - self._last)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = time.monotonic()

async def get_json(url: str, *, headers: Optional[Dict[str, str]] = None,
                   params: Optional[Dict[str, Any]] = None, timeout: float = 20.0) -> Dict[str, Any] | list:
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(url, headers=headers, params=params)
        resp.raise_for_status()
        return resp.json()
